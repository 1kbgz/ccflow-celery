from typing import Any

from ccflow import BaseModel
from celery import Celery
from pydantic import Field

__all__ = (
    "CeleryApp",
    "CeleryConfig",
)


class CeleryConfig(BaseModel):
    """Configuration for a Celery application."""

    broker_url: str = Field(default="redis://localhost:6379/0", description="Celery broker URL")
    result_backend: str | None = Field(default="redis://localhost:6379/0", description="Celery result backend URL")
    task_serializer: str = "json"
    result_serializer: str = "json"
    accept_content: list = Field(default_factory=lambda: ["json"])
    task_track_started: bool = True
    task_default_queue: str = "default"
    task_routes: dict[str, str] | None = None
    worker_concurrency: int | None = None
    task_always_eager: bool = Field(default=False, description="If True, tasks execute locally without a broker")
    task_eager_propagates: bool = Field(default=True, description="If True, eager tasks propagate exceptions")


class CeleryApp(BaseModel):
    """Wrapper around a Celery application instance."""

    name: str = Field(default="ccflow", description="Celery app name")
    config: CeleryConfig = Field(default_factory=CeleryConfig)

    _app: Any = None

    def get_app(self):
        if self._app is None:
            self._app = Celery(self.name)
            self._app.conf.update(
                broker_url=self.config.broker_url,
                result_backend=self.config.result_backend,
                task_serializer=self.config.task_serializer,
                result_serializer=self.config.result_serializer,
                accept_content=self.config.accept_content,
                task_track_started=self.config.task_track_started,
                task_default_queue=self.config.task_default_queue,
            )
            if self.config.task_routes:
                self._app.conf.task_routes = self.config.task_routes
            if self.config.worker_concurrency:
                self._app.conf.worker_concurrency = self.config.worker_concurrency
            if self.config.task_always_eager:
                self._app.conf.task_always_eager = True
                self._app.conf.task_eager_propagates = self.config.task_eager_propagates
            # Register the default task so it can be invoked by name
            # (required for eager mode; harmless in broker mode)
            from .tasks import execute_model_task

            self._app.task(execute_model_task, name="ccflow_celery.tasks.execute_model_task")
        return self._app
