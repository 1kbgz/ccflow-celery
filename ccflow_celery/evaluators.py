import graphlib
from logging import getLogger
from typing import Any, Optional

from ccflow import EvaluatorBase, GenericResult
from ccflow.callable import ModelEvaluationContext
from ccflow.evaluators.common import get_dependency_graph
from celery import group
from pydantic import Field, PrivateAttr
from typing_extensions import override

from .app import CeleryApp

__all__ = (
    "CeleryEvaluator",
    "CeleryGraphEvaluator",
)

_log = getLogger(__name__)


def _resolve_origin(cls):
    """Resolve parameterized generics (e.g. GenericContext[int]) to their origin class."""
    meta = getattr(cls, "__pydantic_generic_metadata__", None)
    if meta and meta.get("origin") is not None:
        return meta["origin"]
    return cls


def _model_fqn(model) -> str:
    """Get the fully qualified class name of a model."""
    cls = _resolve_origin(type(model))
    return f"{cls.__module__}.{cls.__qualname__}"


def _context_fqn(context) -> str:
    """Get the fully qualified class name of a context."""
    cls = _resolve_origin(type(context))
    return f"{cls.__module__}.{cls.__qualname__}"


def _serialize_model(model):
    """Serialize a model to (fqn, config_dict) for Celery task dispatch."""
    fqn = _model_fqn(model)
    config = model.model_dump(exclude={"type_"}) if hasattr(model, "model_dump") else {}
    return fqn, config


def _serialize_context(ctx):
    """Serialize a context to (fqn, config_dict) for Celery task dispatch."""
    fqn = _context_fqn(ctx)
    config = ctx.model_dump(exclude={"type_"}) if hasattr(ctx, "model_dump") else {}
    return fqn, config


def _unwrap_eval_ctx(eval_ctx):
    """Unwrap evaluator-wrapping layers to get the actual model and context.

    The dependency graph may wrap nodes as ModelEvaluationContext(model=evaluator,
    context=ModelEvaluationContext(model=actual_model, context=actual_context)).
    """
    inner = eval_ctx
    while isinstance(inner.context, ModelEvaluationContext):
        inner = inner.context
    return inner.model, inner.context


class CeleryEvaluator(EvaluatorBase):
    """Evaluator that dispatches model execution to Celery workers.

    Serializes the model and context, submits as a Celery task,
    and waits for the result. The model must be reconstructable from its
    Pydantic config dump.
    """

    app: CeleryApp = Field(default_factory=CeleryApp)
    timeout: Optional[float] = Field(default=300.0, description="Timeout in seconds for waiting on task result")
    task_name: str = Field(default="ccflow_celery.tasks.execute_model_task", description="Registered Celery task name")

    _celery_app: Any = PrivateAttr(default=None)

    def _get_celery_app(self):
        if self._celery_app is None:
            self._celery_app = self.app.get_app()
        return self._celery_app

    @override
    def __call__(self, context: ModelEvaluationContext) -> Any:
        model = context.model
        ctx = context.context

        # For __deps__ calls or non-__call__ calls, execute locally
        if hasattr(context, "fn") and context.fn != "__call__":
            return context()

        # Serialize model and context
        model_class, model_config = _serialize_model(model)
        context_class, context_config = _serialize_context(ctx)

        celery_app = self._get_celery_app()

        _log.info("Submitting Celery task: %s(%s)", model_class, context_class)

        # Send task to Celery
        task = celery_app.send_task(
            self.task_name,
            args=[model_class, model_config, context_class, context_config],
        )

        # Wait for result
        result_data = task.get(timeout=self.timeout)
        _log.info("Celery task completed: %s", task.id)

        # Reconstruct result
        if hasattr(model, "result_type"):
            result_cls = model.result_type
            try:
                return result_cls(**result_data)
            except Exception:
                pass

        return GenericResult(value=result_data)


class CeleryGraphEvaluator(EvaluatorBase):
    """Evaluator that parallelizes DAG execution via Celery.

    Builds the dependency graph (like GraphEvaluator), then submits
    independent nodes as parallel Celery tasks using Celery groups.
    Nodes that depend on other nodes wait for their dependencies first.
    """

    app: CeleryApp = Field(default_factory=CeleryApp)
    timeout: Optional[float] = Field(default=600.0, description="Timeout for the entire graph execution")
    task_name: str = Field(default="ccflow_celery.tasks.execute_model_task", description="Registered Celery task name")

    _celery_app: Any = PrivateAttr(default=None)
    _is_evaluating: bool = PrivateAttr(False)

    def _get_celery_app(self):
        if self._celery_app is None:
            self._celery_app = self.app.get_app()
        return self._celery_app

    @override
    def __call__(self, context: ModelEvaluationContext) -> Any:
        # Avoid re-entrancy
        if self._is_evaluating:
            return context()

        self._is_evaluating = True
        root_result = None

        try:
            graph = get_dependency_graph(context)
            ts = graphlib.TopologicalSorter(graph.graph)

            celery_app = self._get_celery_app()
            results_map = {}

            # Process nodes level by level for maximum parallelism
            ts.prepare()
            while ts.is_active():
                ready_nodes = list(ts.get_ready())

                # Submit all ready nodes in parallel
                tasks = []
                for key in ready_nodes:
                    eval_ctx = graph.ids[key]
                    model, ctx = _unwrap_eval_ctx(eval_ctx)

                    model_class, model_config = _serialize_model(model)
                    context_class, context_config = _serialize_context(ctx)

                    sig = celery_app.signature(
                        self.task_name,
                        args=[model_class, model_config, context_class, context_config],
                    )
                    tasks.append((key, sig))

                if len(tasks) == 1:
                    # Single task — submit directly
                    key, sig = tasks[0]
                    async_result = sig.apply_async()
                    result = async_result.get(timeout=self.timeout)
                    results_map[key] = result
                    if key == graph.root_id:
                        root_result = result
                    ts.done(key)
                else:
                    # Multiple tasks — use Celery group for parallelism
                    keys = [k for k, _ in tasks]
                    sigs = [s for _, s in tasks]
                    group_result = group(sigs).apply_async()
                    group_results = group_result.get(timeout=self.timeout)

                    for key, result in zip(keys, group_results):
                        results_map[key] = result
                        if key == graph.root_id:
                            root_result = result
                        ts.done(key)

        finally:
            self._is_evaluating = False

        # Reconstruct result for the root node
        if root_result is not None and hasattr(context, "model"):
            model = context.model
            if hasattr(model, "result_type"):
                result_cls = model.result_type
                try:
                    return result_cls(**root_result)
                except Exception:
                    pass

        return GenericResult(value=root_result)
