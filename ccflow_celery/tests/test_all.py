<<<<<<< before updating
from unittest.mock import MagicMock, patch
=======
from ccflow_celery import *
>>>>>>> after updating

import pytest
from ccflow import CallableModel, Flow, GenericResult, NullContext

from ccflow_celery import *
from ccflow_celery.app import CeleryApp, CeleryConfig
from ccflow_celery.evaluators import CeleryEvaluator, CeleryGraphEvaluator, _context_fqn, _model_fqn
from ccflow_celery.tasks import execute_model_task


class TestCeleryConfig:
    def test_defaults(self):
        cfg = CeleryConfig()
        assert cfg.broker_url == "redis://localhost:6379/0"
        assert cfg.result_backend == "redis://localhost:6379/0"
        assert cfg.task_serializer == "json"
        assert cfg.result_serializer == "json"
        assert cfg.accept_content == ["json"]
        assert cfg.task_track_started is True
        assert cfg.task_default_queue == "default"
        assert cfg.task_routes is None
        assert cfg.worker_concurrency is None

    def test_custom(self):
        cfg = CeleryConfig(broker_url="amqp://broker", task_routes={"t": "q"}, worker_concurrency=4)
        assert cfg.broker_url == "amqp://broker"
        assert cfg.task_routes == {"t": "q"}
        assert cfg.worker_concurrency == 4


class TestCeleryApp:
    def test_defaults(self):
        app = CeleryApp()
        assert app.name == "ccflow"
        assert isinstance(app.config, CeleryConfig)

    @patch("ccflow_celery.app.Celery")
    def test_get_app_creates_celery(self, MockCelery):
        mock_instance = MagicMock()
        MockCelery.return_value = mock_instance
        app = CeleryApp(name="myapp")
        result = app.get_app()
        MockCelery.assert_called_once_with("myapp")
        mock_instance.conf.update.assert_called_once()
        assert result is mock_instance

    @patch("ccflow_celery.app.Celery")
    def test_get_app_caches(self, MockCelery):
        mock_instance = MagicMock()
        MockCelery.return_value = mock_instance
        app = CeleryApp()
        first = app.get_app()
        second = app.get_app()
        assert first is second
        MockCelery.assert_called_once()

    @patch("ccflow_celery.app.Celery")
    def test_get_app_applies_task_routes(self, MockCelery):
        mock_instance = MagicMock()
        MockCelery.return_value = mock_instance
        cfg = CeleryConfig(task_routes={"task.name": "queue"})
        app = CeleryApp(config=cfg)
        app.get_app()
        assert mock_instance.conf.task_routes == {"task.name": "queue"}

    @patch("ccflow_celery.app.Celery")
    def test_get_app_applies_worker_concurrency(self, MockCelery):
        mock_instance = MagicMock()
        MockCelery.return_value = mock_instance
        cfg = CeleryConfig(worker_concurrency=8)
        app = CeleryApp(config=cfg)
        app.get_app()
        assert mock_instance.conf.worker_concurrency == 8


class _DummyContext(NullContext):
    pass


class _DummyModel(CallableModel):
    @Flow.call
    def __call__(self, context: _DummyContext) -> GenericResult:
        return GenericResult(value=None)


class TestFQNHelpers:
    def test_model_fqn(self):
        fqn = _model_fqn(_DummyModel())
        assert fqn.endswith("_DummyModel")
        assert __name__ in fqn or "test_all" in fqn

    def test_context_fqn(self):
        fqn = _context_fqn(_DummyContext())
        assert fqn.endswith("_DummyContext")


class TestExecuteModelTask:
    def test_basic_execution(self):
        """Round-trip: serialise a simple model+context, execute in the task."""
        model = _DummyModel()
        ctx = _DummyContext()
        model_fqn = _model_fqn(model)
        ctx_fqn = _context_fqn(ctx)

        result = execute_model_task(model_fqn, model.model_dump(), ctx_fqn, ctx.model_dump())
        assert isinstance(result, dict)

    def test_result_has_model_dump(self):
        """If the model returns a ResultBase, execute_model_task returns model_dump()."""
        model = _DummyModel()
        ctx = _DummyContext()
        result = execute_model_task(
            _model_fqn(model),
            model.model_dump(),
            _context_fqn(ctx),
            ctx.model_dump(),
        )
        assert isinstance(result, dict)

    def test_bad_class_path_raises(self):
        with pytest.raises((ModuleNotFoundError, AttributeError)):
            execute_model_task("no.such.Module", {}, "no.such.Context", {})


class _FakeEvalContext:
    """Mimics ModelEvaluationContext for unit testing."""

    def __init__(self, model, context, fn="__call__"):
        self.model = model
        self.context = context
        self.fn = fn
        self.options = {}

    def __call__(self):
        return getattr(self.model, self.fn)(self.context) if self.fn == "__call__" else getattr(self.model, self.fn)()


class TestCeleryEvaluator:
    def _make_evaluator(self):
        return CeleryEvaluator(app=CeleryApp())

    @patch("ccflow_celery.app.Celery")
    def test_remote_dispatch(self, MockCelery):
        mock_app = MagicMock()
        MockCelery.return_value = mock_app
        mock_task = MagicMock()
        mock_task.get.return_value = {"value": 42}
        mock_app.send_task.return_value = mock_task

        ev = self._make_evaluator()
        model = _DummyModel()
        ctx = _DummyContext()
        eval_ctx = _FakeEvalContext(model, ctx)

        result = ev(eval_ctx)

        mock_app.send_task.assert_called_once()
        call_args = mock_app.send_task.call_args
        assert call_args[0][0] == "ccflow_celery.tasks.execute_model_task"
        assert isinstance(result, GenericResult)

    def test_non_call_fn_runs_locally(self):
        """When fn != '__call__', CeleryEvaluator executes locally."""
        ev = self._make_evaluator()
        model = _DummyModel()
        ctx = _DummyContext()
        eval_ctx = _FakeEvalContext(model, ctx, fn="__call__")
        # Override fn to a non-__call__ to trigger local execution
        eval_ctx.fn = "__deps__"
        # __deps__ doesn't exist on the model, so let's mock it
        model.__deps__ = MagicMock(return_value=[])
        eval_ctx.model = model
        eval_ctx.__call__ = lambda: model.__deps__()

        _ = ev(eval_ctx)
        model.__deps__.assert_called_once()

    @patch("ccflow_celery.app.Celery")
    def test_result_reconstruction_with_result_type(self, MockCelery):
        """When model has result_type, evaluator reconstructs from it."""
        mock_app = MagicMock()
        MockCelery.return_value = mock_app
        mock_task = MagicMock()
        mock_task.get.return_value = {"value": "reconstructed"}
        mock_app.send_task.return_value = mock_task

        ev = self._make_evaluator()
        model = _DummyModel()
        ctx = _DummyContext()
        eval_ctx = _FakeEvalContext(model, ctx)

        result = ev(eval_ctx)
        # DummyModel inherits result_type from CallableModel → GenericResult
        assert isinstance(result, GenericResult)
        assert result.value == "reconstructed"

    @patch("ccflow_celery.app.Celery")
    def test_celery_app_caching(self, MockCelery):
        mock_app = MagicMock()
        MockCelery.return_value = mock_app
        mock_task = MagicMock()
        mock_task.get.return_value = {"value": None}
        mock_app.send_task.return_value = mock_task

        ev = self._make_evaluator()
        model = _DummyModel()
        ctx = _DummyContext()

        ev(_FakeEvalContext(model, ctx))
        ev(_FakeEvalContext(model, ctx))

        # Celery instance created only once via CeleryApp.get_app()
        MockCelery.assert_called_once()


class TestCeleryGraphEvaluator:
    def _make_evaluator(self):
        return CeleryGraphEvaluator(app=CeleryApp())

    def test_reentrant_calls_locally(self):
        """When _is_evaluating is True, graph evaluator executes context() directly."""
        ev = self._make_evaluator()
        ev._is_evaluating = True

        sentinel = object()
        eval_ctx = MagicMock()
        eval_ctx.return_value = sentinel

        result = ev(eval_ctx)
        eval_ctx.assert_called_once()
        assert result is sentinel

    @patch("ccflow_celery.evaluators.get_dependency_graph")
    @patch("ccflow_celery.app.Celery")
    def test_single_node_graph(self, MockCelery, mock_get_dep):
        """Graph with one node dispatches via apply_async directly (no group)."""
        mock_app = MagicMock()
        MockCelery.return_value = mock_app

        model = _DummyModel()
        ctx = _DummyContext()
        eval_ctx_inner = _FakeEvalContext(model, ctx)

        # Build a mock dependency graph with one node
        mock_graph = MagicMock()
        root_key = "root_id"
        mock_graph.graph = {root_key: set()}  # one node, no deps
        mock_graph.ids = {root_key: eval_ctx_inner}
        mock_graph.root_id = root_key
        mock_get_dep.return_value = mock_graph

        # Mock signature and async result
        mock_sig = MagicMock()
        mock_async = MagicMock()
        mock_async.get.return_value = {"value": "root_val"}
        mock_sig.apply_async.return_value = mock_async
        mock_app.signature.return_value = mock_sig

        ev = self._make_evaluator()
        outer_ctx = _FakeEvalContext(model, ctx)

        result = ev(outer_ctx)

        mock_app.signature.assert_called_once()
        mock_sig.apply_async.assert_called_once()
        assert isinstance(result, GenericResult)

    @patch("ccflow_celery.evaluators.get_dependency_graph")
    @patch("ccflow_celery.evaluators.group")
    @patch("ccflow_celery.app.Celery")
    def test_parallel_nodes_use_group(self, MockCelery, mock_group, mock_get_dep):
        """Multiple independent nodes at the same level use Celery group."""
        mock_app = MagicMock()
        MockCelery.return_value = mock_app

        model = _DummyModel()
        ctx = _DummyContext()

        # Two independent leaf nodes, then a root depending on both
        leaf1_ctx = _FakeEvalContext(model, ctx)
        leaf2_ctx = _FakeEvalContext(model, ctx)
        root_ctx = _FakeEvalContext(model, ctx)

        mock_graph = MagicMock()
        mock_graph.graph = {
            "root": {"leaf1", "leaf2"},
            "leaf1": set(),
            "leaf2": set(),
        }
        mock_graph.ids = {
            "leaf1": leaf1_ctx,
            "leaf2": leaf2_ctx,
            "root": root_ctx,
        }
        mock_graph.root_id = "root"
        mock_get_dep.return_value = mock_graph

        # Mock signature
        mock_sig = MagicMock()
        mock_app.signature.return_value = mock_sig

        # Mock group result for the two leaves
        mock_group_result = MagicMock()
        mock_group_result.get.return_value = [{"value": "l1"}, {"value": "l2"}]
        mock_group_instance = MagicMock()
        mock_group_instance.apply_async.return_value = mock_group_result
        mock_group.return_value = mock_group_instance

        # Root will be a single task
        mock_root_async = MagicMock()
        mock_root_async.get.return_value = {"value": "root_val"}
        mock_sig.apply_async.return_value = mock_root_async

        ev = self._make_evaluator()
        outer_ctx = _FakeEvalContext(model, ctx)

        result = ev(outer_ctx)

        # group() should have been called for the parallel leaf nodes
        mock_group.assert_called_once()
        assert isinstance(result, GenericResult)

    @patch("ccflow_celery.evaluators.get_dependency_graph")
    @patch("ccflow_celery.app.Celery")
    def test_is_evaluating_reset_on_exception(self, MockCelery, mock_get_dep):
        """_is_evaluating is reset even when graph processing raises."""
        mock_app = MagicMock()
        MockCelery.return_value = mock_app
        mock_get_dep.side_effect = RuntimeError("boom")

        ev = self._make_evaluator()
        model = _DummyModel()
        ctx = _DummyContext()
        outer_ctx = _FakeEvalContext(model, ctx)

        with pytest.raises(RuntimeError, match="boom"):
            ev(outer_ctx)

        assert ev._is_evaluating is False
