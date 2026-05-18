"""Canonical Pydantic models for renderer output and diff documents.

Both the renderer subprocess and the parent orchestrator validate against these
types. The schema is versioned; bump SCHEMA_VERSION when changing wire shape.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = 2


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


# ----- Renderer output ----------------------------------------------------

class ProvenanceEntry(_Model):
    source: Literal["literal", "stub", "fixture"]
    key: Optional[str] = None  # e.g. "var.value.bucket"; None for literal


class RenderedField(_Model):
    rendered: Any  # the post-render Python value (str, int, dict, list, ...)
    provenance: list[ProvenanceEntry] = Field(default_factory=list)


class ExternalTaskRef(_Model):
    """Cross-DAG metadata captured from an ExternalTaskSensor instance."""
    kind: Literal["external_task_sensor"]
    external_dag_id: str
    external_task_id: Optional[str] = None
    external_task_ids: Optional[list[str]] = None
    external_task_group_id: Optional[str] = None
    execution_delta_seconds: Optional[int] = None
    execution_date_fn_present: bool = False

    @model_validator(mode="after")
    def _check_single_target(self) -> "ExternalTaskRef":
        set_fields = sum([
            self.external_task_id is not None,
            self.external_task_ids is not None,
            self.external_task_group_id is not None,
        ])
        if set_fields > 1:
            raise ValueError(
                "At most one of external_task_id, external_task_ids, "
                "external_task_group_id may be set"
            )
        return self


DagStatus = Literal["ok", "error"]


class RenderError(_Model):
    type: str
    message: str
    traceback: str


class RenderedTask(_Model):
    task_id: str
    operator: str  # fully-qualified class name
    task_group: Optional[str] = None  # group_id of parent TaskGroup, or None
    upstream: list[str] = Field(default_factory=list)
    downstream: list[str] = Field(default_factory=list)
    fields: dict[str, RenderedField] = Field(default_factory=dict)
    external_ref: Optional[ExternalTaskRef] = None


class TaskGroupInfo(_Model):
    group_id: str
    tasks: list[str]  # task_ids belonging directly to this group


class DatasetRefs(_Model):
    inlets: list[str] = Field(default_factory=list)
    outlets: list[str] = Field(default_factory=list)


class RenderedDag(_Model):
    dag_id: str
    status: DagStatus
    source_file: str
    # Present only when status == "ok":
    attrs: Optional[dict[str, Any]] = None
    datasets: Optional[DatasetRefs] = None
    task_groups: Optional[list[TaskGroupInfo]] = None
    tasks: Optional[list[RenderedTask]] = None
    # Present only when status == "error":
    error: Optional[RenderError] = None

    @model_validator(mode="after")
    def _check_status_field_invariant(self) -> "RenderedDag":
        if self.status == "ok" and self.error is not None:
            raise ValueError(
                "RenderedDag with status='ok' must not have an error field set"
            )
        if self.status == "error" and any(
            field is not None
            for field in (self.attrs, self.datasets, self.task_groups, self.tasks)
        ):
            raise ValueError(
                "RenderedDag with status='error' must not have attrs, datasets, "
                "task_groups, or tasks populated"
            )
        return self


class RenderedDagBag(_Model):
    schema_version: Literal[2]
    commit_sha: str
    airflow_version: str
    rendered_at: AwareDatetime
    dags: list[RenderedDag]


# ----- Diff document ------------------------------------------------------

ChangeType = Literal["added", "removed", "modified", "unchanged"]
DagClassification = Literal["touched", "incidentally_affected", "added", "removed"]
DagPairStatus = Literal["ok", "regressed", "fixed", "still_broken"]


class FieldDiff(_Model):
    name: str
    change_type: ChangeType
    before: Any = None
    after: Any = None
    provenance_before: list[ProvenanceEntry] = Field(default_factory=list)
    provenance_after: list[ProvenanceEntry] = Field(default_factory=list)
    render_error_before: Optional[RenderError] = None
    render_error_after: Optional[RenderError] = None


class EdgeDiff(_Model):
    direction: Literal["upstream", "downstream"]
    change_type: Literal["added", "removed"]
    task_id: str
    related_task_id: str  # the other end of the edge


class AttrDiff(_Model):
    name: str
    before: Any = None
    after: Any = None


class TaskDiff(_Model):
    task_id: str
    change_type: ChangeType
    operator_before: Optional[str] = None
    operator_after: Optional[str] = None
    field_diffs: list[FieldDiff] = Field(default_factory=list)
    edge_diffs: list[EdgeDiff] = Field(default_factory=list)


class DagDiff(_Model):
    dag_id: str
    classification: DagClassification
    status_a: Optional[DagStatus] = None
    status_b: Optional[DagStatus] = None
    pair_status: DagPairStatus = "ok"
    source_file_before: Optional[str] = None
    source_file_after: Optional[str] = None
    attr_diffs: list[AttrDiff] = Field(default_factory=list)
    task_diffs: list[TaskDiff] = Field(default_factory=list)
    error_before: Optional[RenderError] = None
    error_after: Optional[RenderError] = None


class DiffSummary(_Model):
    dags_touched: int = 0
    dags_incidentally_affected: int = 0
    dags_added: int = 0
    dags_removed: int = 0
    dags_regressed: int = 0
    dags_fixed: int = 0


class RenderErrorEntry(_Model):
    dag_id: str
    side: Literal["base", "head", "both"]
    error_base: Optional[RenderError] = None
    error_head: Optional[RenderError] = None


class SensorMismatch(_Model):
    sensor_dag_id: str
    sensor_task_id: str
    target_dag_id: str
    target_task_id: Optional[str] = None
    target_task_ids: Optional[list[str]] = None
    reason: Literal[
        "missing_execution_delta",
        "incorrect_execution_delta",
        "dangling_target",
    ]
    sensor_schedule: Optional[str] = None
    target_schedule: Optional[str] = None
    expected_delta_seconds: Optional[int] = None
    actual_delta_seconds: Optional[int] = None
    notes: Optional[str] = Field(None, max_length=500)

    @model_validator(mode="after")
    def _check_reason_field_invariant(self) -> "SensorMismatch":
        if self.reason == "incorrect_execution_delta":
            if self.expected_delta_seconds is None or self.actual_delta_seconds is None:
                raise ValueError(
                    "SensorMismatch with reason='incorrect_execution_delta' must "
                    "have expected_delta_seconds and actual_delta_seconds set"
                )
        return self


class DiffDocument(_Model):
    schema_version: Literal[2]
    base_sha: str
    head_sha: str
    summary: DiffSummary
    dags: list[DagDiff]
    render_errors: list[RenderErrorEntry] = Field(default_factory=list)
    sensor_mismatches: list[SensorMismatch] = Field(default_factory=list)
