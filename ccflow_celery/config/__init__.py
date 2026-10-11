from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ccflow.utils.hydra import ConfigLoadResult

__all__ = ("load_config",)


def load_config(
    overrides: list[str] | None = None,
    overwrite: bool = False,
    config_dir: str | None = None,
    config_key: str | None = None,
    version_base: str | None = None,
) -> ConfigLoadResult:
    # Imported here so lerna can locate this config package without importing ccflow
    from ccflow import ModelRegistry
    from ccflow.utils.hydra import load_config as base_load_config

    parent_dir = str(Path(__file__).resolve().parent)
    result = base_load_config(
        root_config_dir=parent_dir,
        root_config_name="base",
        config_dir=config_dir,
        overrides=overrides,
        version_base=version_base,
        basepath=parent_dir,
        debug=False,
    )
    cfg = result.cfg
    if config_key is not None:
        cfg = cfg[config_key]
    r = ModelRegistry.root()
    r.load_config(cfg, overwrite=overwrite)
    return r
