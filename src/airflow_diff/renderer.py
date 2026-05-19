"""DAG-bag renderer (runs inside a per-commit subprocess).

Imports the DAG bag at `--worktree`, renders all template fields against a
synthetic Jinja context with stubbed Variables/Connections/XCom, and emits a
RenderedDagBag JSON document on stdout. This is the ONLY module that imports
Airflow.

Run as: python -m airflow_diff.renderer --worktree <path> --commit-sha <sha> \\
                                        --config <json> [--fixtures <yaml>]
"""

from __future__ import annotations

import argparse
import fnmatch
import importlib.util
import inspect
import json
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow_diff.schema import ExternalTaskRef, RenderedDag

# Names that should never appear in the rendered literal-kwargs set, either
# because they're captured at a higher level (DAG/datasets), are purely
# cosmetic, or are documentation strings that bloat diffs without adding
# behavioral signal.
_LITERAL_BLOCKLIST = frozenset(
    {
        # Structural — captured at DAG/task-group/dataset level, not here:
        "dag",
        "task_group",
        "task_id",
        "inlets",
        "outlets",
        "params",
        "default_args",
        "subdag",
        # Cosmetic:
        "ui_color",
        "ui_fgcolor",
        # Documentation (noisy, not behavioral):
        "doc",
        "doc_md",
        "doc_json",
        "doc_yaml",
        "doc_rst",
        # User identity (changes per-task usage but not behavior):
        "owner",
    }
)

# Stubs MUST be installed before any DAG import. We patch Airflow's Variable,
# BaseHook, and TaskInstance.xcom_pull at module level.

_FIXTURES: dict[str, Any] = {"variables": {}, "connections": {}}


def _install_stubs() -> None:
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    from airflow.models.connection import Connection

    def stub_variable_get(key, default_var=None, deserialize_json=False, **kw):
        if key in _FIXTURES["variables"]:
            v = _FIXTURES["variables"][key]
            if deserialize_json and isinstance(v, str):
                return json.loads(v)
            return v
        return f"<VAR:{key}>"

    Variable.get = staticmethod(stub_variable_get)

    def stub_get_connection(conn_id):
        fix = _FIXTURES["connections"].get(conn_id)
        if fix:
            return Connection(conn_id=conn_id, **fix)
        return Connection(
            conn_id=conn_id,
            host=f"<CONN:{conn_id}.host>",
            schema=f"<CONN:{conn_id}.schema>",
            login=f"<CONN:{conn_id}.login>",
            password=f"<CONN:{conn_id}.password>",
            port=0,
            extra="{}",
        )

    BaseHook.get_connection = staticmethod(stub_get_connection)

    # xcom_pull stub (patched on TaskInstance class)
    from airflow.models.taskinstance import TaskInstance

    def stub_xcom_pull(self, task_ids=None, key="return_value", **kw):
        ids = task_ids if isinstance(task_ids, str) else ",".join(task_ids or [])
        return f"<XCOM:{ids}.{key}>"

    TaskInstance.xcom_pull = stub_xcom_pull


def _airflow_version_ok() -> tuple[bool, str]:
    import airflow

    v = airflow.__version__
    major = int(v.split(".")[0])
    return major == 2, v


def _import_dag_file(path: Path) -> dict[str, Any]:
    """Return module globals after importing `path`. Raises on import error."""
    spec = importlib.util.spec_from_file_location(
        f"airflow_diff_user_dag_{path.stem}_{abs(hash(str(path)))}", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__dict__


def _build_context(dag, task, synthetic_logical_date: str) -> dict[str, Any]:
    """Build a Jinja context similar to what the scheduler injects at task run."""
    dt = datetime.fromisoformat(synthetic_logical_date)
    ds = dt.strftime("%Y-%m-%d")
    return {
        "ds": ds,
        "ds_nodash": ds.replace("-", ""),
        "ts": dt.isoformat(),
        "ts_nodash": dt.isoformat().replace("-", "").replace(":", ""),
        "logical_date": dt,
        "execution_date": dt,
        "dag": dag,
        "task": task,
        "ti": _StubTI(),
        "task_instance": _StubTI(),
        "params": dag.params if hasattr(dag, "params") else {},
        "var": _StubVarNamespace(),
        "conn": _StubConnNamespace(),
        "macros": __import__("airflow.macros", fromlist=["*"]),
    }


class _StubVarNamespace:
    class _ValueProxy:
        def __getattr__(self, name):
            return _FIXTURES["variables"].get(name, f"<VAR:{name}>")

        def __getitem__(self, name):
            return _FIXTURES["variables"].get(name, f"<VAR:{name}>")

    class _JsonProxy:
        def __getattr__(self, name):
            return _FIXTURES["variables"].get(name, f"<VAR:{name}>")

        def __getitem__(self, name):
            return _FIXTURES["variables"].get(name, f"<VAR:{name}>")

    value = _ValueProxy()
    json = _JsonProxy()


class _StubConnNamespace:
    def __getattr__(self, name):
        return _StubConnEntry(name)


class _StubConnEntry:
    def __init__(self, conn_id):
        self._id = conn_id

    def __getattr__(self, name):
        return f"<CONN:{self._id}.{name}>"


class _StubTI:
    def xcom_pull(self, task_ids=None, key="return_value", **kw):
        ids = task_ids if isinstance(task_ids, str) else ",".join(task_ids or [])
        return f"<XCOM:{ids}.{key}>"


def _extract_literal_kwargs(task, template_fields: frozenset) -> dict[str, Any]:
    """Capture non-template operator kwargs the user set to non-default values.

    Walks the operator class's MRO and inspects each level's `__init__` signature
    to discover every named parameter. For each, the corresponding attribute on
    the task instance is captured iff:

      * the name is not in `template_fields` (already rendered via Jinja),
      * the name is not in `_LITERAL_BLOCKLIST` (structural/cosmetic/doc),
      * the value is not None,
      * the value is not callable (callbacks can't be diffed),
      * the value does not equal the parameter's declared default (per the
        signature; user didn't override it).

    Returns a dict mapping name → JSON-safe value (via `_jsonify`).
    """
    params: dict[str, inspect.Parameter] = {}
    for cls in type(task).__mro__:
        try:
            sig = inspect.signature(cls.__init__)
        except (ValueError, TypeError):
            continue
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            # More-derived class wins (we iterate MRO from most-derived).
            params.setdefault(name, param)

    out: dict[str, Any] = {}
    for name, param in params.items():
        if name in template_fields:
            continue
        if name in _LITERAL_BLOCKLIST:
            continue
        try:
            value = getattr(task, name)
        except AttributeError:
            continue
        if value is None:
            continue
        # Skip anything _jsonify can't handle without falling through to repr().
        # repr() of arbitrary objects embeds memory addresses, which produce
        # spurious diffs across renderer runs (e.g., weight_rule strategy
        # instances render as "<...object at 0x7f...>"). This also catches
        # callables (functions, lambdas, methods, partials).
        if not isinstance(
            value, (str, int, float, bool, list, tuple, dict, set, frozenset, datetime, timedelta)
        ):
            continue
        if param.default is not inspect.Parameter.empty:
            try:
                if value == param.default:
                    continue
            except Exception:
                # Some objects don't support __eq__ safely; fall through and
                # capture the value rather than crashing.
                pass
        out[name] = _jsonify(value)
    return out


def _extract_external_ref(task) -> ExternalTaskRef | None:
    """Capture cross-DAG metadata for any task whose MRO contains ExternalTaskSensor.

    Uses class-name MRO walk rather than isinstance to avoid hard-importing
    airflow.sensors.external_task (defensive, matches _extract_dataset_uris style).
    """
    from datetime import timedelta as _td

    from airflow_diff.schema import ExternalTaskRef

    if not any(c.__name__ == "ExternalTaskSensor" for c in type(task).__mro__):
        return None

    delta = getattr(task, "execution_delta", None)
    delta_seconds = int(delta.total_seconds()) if isinstance(delta, _td) else None

    external_task_id = getattr(task, "external_task_id", None)
    # Airflow 2.10 may mirror external_task_id into external_task_ids; only set
    # external_task_ids when external_task_id is absent to satisfy the schema's
    # single-target invariant.
    if external_task_id is not None:
        external_task_ids = None
    else:
        external_task_ids = list(getattr(task, "external_task_ids", []) or []) or None

    return ExternalTaskRef(
        kind="external_task_sensor",
        external_dag_id=getattr(task, "external_dag_id", "") or "",
        external_task_id=external_task_id,
        external_task_ids=external_task_ids,
        external_task_group_id=getattr(task, "external_task_group_id", None),
        execution_delta_seconds=delta_seconds,
        execution_date_fn_present=callable(getattr(task, "execution_date_fn", None)),
    )


def _render_dag(dag, synthetic_logical_date: str) -> RenderedDag:
    """Walk a DAG, render templates per task, return a RenderedDag."""
    from airflow_diff.schema import (
        DatasetRefs,
        ProvenanceEntry,
        RenderedDag,
        RenderedField,
        RenderedTask,
        TaskGroupInfo,
    )

    tasks_out: list[RenderedTask] = []
    for task in dag.tasks:
        fields: dict[str, RenderedField] = {}
        context = _build_context(dag, task, synthetic_logical_date)
        for fname in task.template_fields or ():
            try:
                value = getattr(task, fname, None)
                rendered = task.render_template(value, context)
            except Exception as e:
                # Field-level errors: record with a placeholder; loop continues.
                # (Bugfix vs plan: removed the dead intermediate RenderedField
                #  assignment; we set the final value directly.)
                fields[fname] = RenderedField(
                    rendered=f"<RENDER_ERROR: {type(e).__name__}>",
                    provenance=[ProvenanceEntry(source="literal")],
                )
                continue
            prov = _classify_provenance(rendered)
            fields[fname] = RenderedField(rendered=_jsonify(rendered), provenance=prov)
        # Capture non-template operator kwargs the user set to non-default
        # values, walking the MRO of the operator class. See _extract_literal_kwargs.
        try:
            literal_kwargs = _extract_literal_kwargs(task, frozenset(task.template_fields or ()))
        except Exception:
            literal_kwargs = {}  # per-task isolation
        for k, v in literal_kwargs.items():
            if k not in fields:
                fields[k] = RenderedField(
                    rendered=v,
                    provenance=[ProvenanceEntry(source="literal")],
                )

        tg_id = task.task_group.group_id if (task.task_group and task.task_group.group_id) else None
        try:
            external_ref = _extract_external_ref(task)
        except Exception:
            external_ref = None  # per-task isolation matches existing policy
        if external_ref is not None:
            # ExternalTaskSensor's cross-DAG kwargs are encoded in external_ref;
            # drop them from `fields` to avoid producing duplicate diff entries
            # (one as a SensorMismatch row, one as a per-field diff). The Jinja
            # template loop also captures these because they're in
            # ExternalTaskSensor.template_fields, so deduplication has to happen
            # after both capture paths have run.
            for _dup in (
                "external_dag_id",
                "external_task_id",
                "external_task_ids",
                "external_task_group_id",
                "execution_delta",
                "execution_date_fn",
            ):
                fields.pop(_dup, None)
        tasks_out.append(
            RenderedTask(
                task_id=task.task_id,
                operator=f"{type(task).__module__}.{type(task).__name__}",
                task_group=tg_id,
                upstream=sorted(t.task_id for t in task.upstream_list),
                downstream=sorted(t.task_id for t in task.downstream_list),
                fields=fields,
                external_ref=external_ref,
            )
        )

    attrs = {
        "schedule": _jsonify(
            getattr(dag, "schedule_interval", None) or getattr(dag, "schedule", None)
        ),
        "start_date": _jsonify(getattr(dag, "start_date", None)),
        "catchup": getattr(dag, "catchup", None),
        "tags": list(getattr(dag, "tags", []) or []),
        "description": getattr(dag, "description", None),
        "max_active_runs": getattr(dag, "max_active_runs", None),
    }
    # Strip None values
    attrs = {k: v for k, v in attrs.items() if v is not None}

    # TaskGroups (flat list of every non-root group)
    task_groups: list[TaskGroupInfo] = []
    try:
        tg_dict = getattr(dag, "task_group_dict", None)
        if tg_dict:
            for tg in tg_dict.values():
                if tg.group_id is None:
                    continue
                task_groups.append(
                    TaskGroupInfo(
                        group_id=tg.group_id,
                        tasks=sorted(
                            t.task_id for t in tg.children.values() if hasattr(t, "task_id")
                        ),
                    )
                )
        else:
            # Fallback: group tasks by task_group.group_id manually
            groups: dict[str, list[str]] = {}
            for t in dag.tasks:
                gid = t.task_group.group_id if (t.task_group and t.task_group.group_id) else None
                if gid:
                    groups.setdefault(gid, []).append(t.task_id)
            for gid, tids in sorted(groups.items()):
                task_groups.append(TaskGroupInfo(group_id=gid, tasks=sorted(tids)))
    except Exception:
        pass

    # Datasets
    datasets = DatasetRefs(
        inlets=sorted(_extract_dataset_uris(getattr(dag, "dataset_triggers", None))),
        outlets=sorted(_extract_dataset_uris(_collect_outlets(dag))),
    )

    return RenderedDag(
        dag_id=dag.dag_id,
        status="ok",
        source_file=str(getattr(dag, "fileloc", "<unknown>")),
        attrs=attrs,
        datasets=datasets,
        task_groups=task_groups,
        tasks=tasks_out,
    )


def _collect_outlets(dag) -> list:
    outlets = []
    for t in dag.tasks:
        outlets.extend(getattr(t, "outlets", []) or [])
    return outlets


def _extract_dataset_uris(items) -> list[str]:
    if not items:
        return []
    # Airflow's dataset_triggers type varies across 2.8/2.9/2.10. On 2.9 it can
    # be a non-iterable BaseDatasetEventInput. Walk defensively: pull a `uri`
    # off the object itself, then attempt iteration. If neither works, return [].
    uris: list[str] = []
    direct = getattr(items, "uri", None)
    if direct:
        uris.append(direct)
    try:
        iterator = iter(items)
    except TypeError:
        return uris
    for it in iterator:
        uri = getattr(it, "uri", None)
        if uri:
            uris.append(uri)
    return uris


def _classify_provenance(value):
    """Best-effort: scan a string for our stub markers and report what was used."""
    from airflow_diff.schema import ProvenanceEntry

    if not isinstance(value, str):
        return [ProvenanceEntry(source="literal")]
    import re

    prov = []
    for m in re.finditer(r"<(VAR|CONN|XCOM):([^>]+)>", value):
        kind = m.group(1)
        key = m.group(2)
        if kind == "VAR":
            prov.append(ProvenanceEntry(source="stub", key=f"var.value.{key}"))
        elif kind == "CONN":
            prov.append(ProvenanceEntry(source="stub", key=f"conn.{key}"))
        elif kind == "XCOM":
            prov.append(ProvenanceEntry(source="stub", key=f"xcom.{key}"))
    if not prov:
        prov.append(ProvenanceEntry(source="literal"))
    return prov


def _jsonify(value):
    """Make any value safe to put in JSON. Stringify datetimes, durations, etc."""
    from datetime import datetime, timedelta

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return f"PT{int(value.total_seconds())}S"
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return repr(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worktree", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--config", required=True, help="JSON-encoded config dict")
    parser.add_argument("--fixtures", default=None, help="path to fixtures YAML")
    args = parser.parse_args(argv)

    worktree = Path(args.worktree)
    config = json.loads(args.config)

    # Load fixtures if provided
    if args.fixtures:
        import yaml

        fix = yaml.safe_load(Path(args.fixtures).read_text()) or {}
        _FIXTURES["variables"] = fix.get("variables") or {}
        _FIXTURES["connections"] = fix.get("connections") or {}

    # Check Airflow version
    ok, version = _airflow_version_ok()
    if not ok:
        print(f"ERROR: airflow-diff requires Airflow 2.x; found {version}", file=sys.stderr)
        return 4

    _install_stubs()

    # Add dags + plugins to path
    dags_folder = worktree / config.get("dags_folder", "dags")
    plugins_folder = worktree / config.get("plugins_folder", "plugins")
    for p in (dags_folder, plugins_folder):
        if p.exists() and str(p) not in sys.path:
            sys.path.insert(0, str(p))

    synthetic_logical_date = config.get("synthetic_logical_date", "2025-01-01T00:00:00+00:00")
    excluded_files = config.get("excluded_files") or []
    excluded_dag_ids = config.get("excluded_dag_ids") or []

    from airflow.models import DAG

    from airflow_diff.schema import (
        SCHEMA_VERSION,
        RenderedDag,
        RenderedDagBag,
        RenderError,
    )

    rendered: list[RenderedDag] = []
    if dags_folder.exists():
        for py in sorted(dags_folder.rglob("*.py")):
            rel_to_dags = str(py.relative_to(dags_folder))
            if any(fnmatch.fnmatch(rel_to_dags, pat) for pat in excluded_files):
                continue
            try:
                globs = _import_dag_file(py)
            except Exception as e:
                rendered.append(
                    RenderedDag(
                        dag_id=py.stem,
                        status="error",
                        source_file=str(py.relative_to(worktree)),
                        error=RenderError(
                            type=type(e).__name__, message=str(e), traceback=traceback.format_exc()
                        ),
                    )
                )
                continue
            for v in globs.values():
                if isinstance(v, DAG):
                    if any(fnmatch.fnmatch(v.dag_id, pat) for pat in excluded_dag_ids):
                        continue
                    try:
                        rendered.append(_render_dag(v, synthetic_logical_date))
                    except Exception as e:
                        rendered.append(
                            RenderedDag(
                                dag_id=v.dag_id,
                                status="error",
                                source_file=str(py.relative_to(worktree)),
                                error=RenderError(
                                    type=type(e).__name__,
                                    message=str(e),
                                    traceback=traceback.format_exc(),
                                ),
                            )
                        )

    import airflow

    bag = RenderedDagBag(
        schema_version=SCHEMA_VERSION,
        commit_sha=args.commit_sha,
        airflow_version=airflow.__version__,
        rendered_at=datetime.now(timezone.utc),
        dags=rendered,
    )
    print(bag.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
