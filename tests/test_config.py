from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from vakya.config import Config
from vakya.exceptions import ConfigError


def test_config_defaults() -> None:
    config = Config.default()
    assert config.audio_column == "audio"
    assert config.audio_format == "flac"
    assert "text" in config.metadata_columns


def test_config_from_yaml(tmp_path: Path) -> None:
    config_data = {
        "audio_column": "my_audio",
        "metadata_columns": ["col1"],
        "workers": 4,
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = Config.from_yaml(config_file)
    assert config.audio_column == "my_audio"
    assert config.metadata_columns == ["col1"]
    assert config.workers <= (os.cpu_count() or 1)


def test_config_from_yaml_missing() -> None:
    with pytest.raises(ConfigError):
        Config.from_yaml("nonexistent.yaml")


def test_config_invalid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        f.write("invalid: yaml: :")

    with pytest.raises(ConfigError):
        Config.from_yaml(config_file)


def test_config_validation() -> None:
    with pytest.raises(ValidationError):
        Config(audio_format="")  # Too short

    with pytest.raises(ValidationError):
        Config(metadata_columns=[])  # Empty list
