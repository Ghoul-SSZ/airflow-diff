from airflow.models.baseoperator import BaseOperator


class GreetingOperator(BaseOperator):
    template_fields = ("greeting", "name")

    def __init__(self, *, greeting: str, name: str, **kwargs):
        super().__init__(**kwargs)
        self.greeting = greeting
        self.name = name

    def execute(self, context):
        return f"{self.greeting}, {self.name}!"
