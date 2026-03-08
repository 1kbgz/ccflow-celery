import importlib
from logging import getLogger

_log = getLogger(__name__)

__all__ = ("execute_model_task",)


def execute_model_task(model_class: str, model_config: dict, context_class: str, context_config: dict) -> dict:
    """Execute a ccflow CallableModel on a Celery worker.

    This is the Celery task function that reconstructs a model and context
    from their serialized configurations and executes the model.

    Args:
        model_class: Fully qualified class name (e.g., "ccflow_s3.S3Model")
        model_config: Dict of model configuration (pydantic model_dump)
        context_class: Fully qualified class name for the context
        context_config: Dict of context configuration (pydantic model_dump)

    Returns:
        Dict representation of the result (model_dump)
    """

    def _import_class(class_path: str):
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    model_cls = _import_class(model_class)
    context_cls = _import_class(context_class)

    model = model_cls(**model_config)
    context = context_cls(**context_config)

    _log.info("Executing %s with context %s", model_class, context_class)
    result = model(context)

    if hasattr(result, "model_dump"):
        # Exclude type_ to avoid ccflow's polymorphic validator interfering on reconstruction
        return result.model_dump(exclude={"type_"})
    return {"value": result}
