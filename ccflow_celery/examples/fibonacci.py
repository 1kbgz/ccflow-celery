"""Fibonacci example using ccflow-celery with local (eager) execution.

Demonstrates:
- Defining a CallableModel with __deps__ for dependency-graph discovery
- Using CeleryGraphEvaluator in eager mode (no message broker required)
- Graph-parallel dispatch of Fibonacci sub-problems

Usage:
    python -m ccflow_celery.examples.fibonacci
"""

from ccflow import CallableModel, Flow, FlowOptionsOverride, GenericContext, GenericResult
from ccflow.evaluators import MemoryCacheEvaluator, MultiEvaluator

from ccflow_celery import CeleryApp, CeleryConfig, CeleryGraphEvaluator

__all__ = (
    "FibonacciModel",
    "main",
)


class FibonacciModel(CallableModel):
    """Compute Fibonacci numbers via ccflow dependency graphs.

    The graph evaluator calls __deps__ to discover the full dependency tree,
    then dispatches each node as a Celery task in topological order.
    """

    @Flow.call
    def __call__(self, context: GenericContext[int]) -> GenericResult[int]:
        n = context.value
        if n <= 1:
            return GenericResult[int](value=n)
        a = self(GenericContext[int](value=n - 1)).value
        b = self(GenericContext[int](value=n - 2)).value
        return GenericResult[int](value=a + b)

    @Flow.deps
    def __deps__(self, context: GenericContext[int]):
        if context.value <= 1:
            return []
        return [
            (
                self,
                [
                    GenericContext[int](value=context.value - 2),
                    GenericContext[int](value=context.value - 1),
                ],
            )
        ]


def main():
    # Eager mode: tasks run in-process, no Redis/RabbitMQ needed
    config = CeleryConfig(
        broker_url="memory://",
        result_backend="cache+memory://",
        task_always_eager=True,
    )
    app = CeleryApp(name="fibonacci", config=config)

    evaluator = MultiEvaluator(
        evaluators=[
            CeleryGraphEvaluator(app=app),
            MemoryCacheEvaluator(),
        ]
    )

    model = FibonacciModel()

    with FlowOptionsOverride(options={"cacheable": True, "evaluator": evaluator}):
        for n in range(11):
            result = model(GenericContext[int](value=n))
            print(f"fib({n}) = {result.value}")


if __name__ == "__main__":
    main()
