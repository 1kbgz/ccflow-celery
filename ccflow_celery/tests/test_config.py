from pathlib import Path

import pytest
from ccflow.evaluators import MultiEvaluator
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from ccflow_celery import CeleryGraphEvaluator

CONFIG_ROOT = Path(__file__).parents[1] / "config"
EVALUATORS = sorted(p.stem for p in (CONFIG_ROOT / "evaluator").glob("*.yaml"))


@pytest.mark.parametrize("name", EVALUATORS)
def test_packaged_evaluator_configs_instantiate(name):
    with initialize_config_dir(config_dir=str(CONFIG_ROOT), version_base=None):
        cfg = compose(config_name="base", overrides=[f"+evaluator={name}"])

    evaluator = instantiate(cfg.cli.model.evaluator)

    assert isinstance(evaluator, MultiEvaluator)
    assert isinstance(evaluator.evaluators[0], CeleryGraphEvaluator)


def test_broker_urls_read_ccflow_celery_env(monkeypatch):
    monkeypatch.setenv("CCFLOW_CELERY_BROKER_URL", "redis://broker:6379/0")
    monkeypatch.setenv("CCFLOW_CELERY_RESULT_BACKEND", "redis://backend:6379/1")
    with initialize_config_dir(config_dir=str(CONFIG_ROOT), version_base=None):
        cfg = compose(config_name="base", overrides=["+evaluator=ccflow_celery"])

    config = instantiate(cfg.cli.model.evaluator).evaluators[0].app.config

    assert config.broker_url == "redis://broker:6379/0"
    assert config.result_backend == "redis://backend:6379/1"
