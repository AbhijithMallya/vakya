from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from .exceptions import ConfigError


class Config(BaseModel):
    """Configuration for the vakya extraction tool."""

    audio_column: str = "audio"
    audio_format: str = "flac"
    filename_column: str = "fname"
    metadata_columns: list[str] = Field(
        default_factory=lambda: [
            "fname",
            "text",
            "duration",
            "gender",
            "speaker_id",
            "lang",
        ]
    )
    master_jsonl: str | None = "master.jsonl"
    workers: int = 1
    overwrite: bool = False
    log_level: str = "INFO"

    @field_validator("audio_format")
    @classmethod
    def validate_audio_format(cls, v: str) -> str:
        """Ensure audio_format is non-empty and alphanumeric."""
        if not v or not v.isalnum():
            raise ValueError("audio_format must be a non-empty alphanumeric string")
        return v

    @field_validator("metadata_columns")
    @classmethod
    def validate_metadata_columns(cls, v: list[str]) -> list[str]:
        """Ensure metadata_columns is a non-empty list of strings."""
        if not v:
            raise ValueError("metadata_columns must be a non-empty list of strings")
        return v

    @model_validator(mode="after")
    def validate_workers(self) -> Config:
        """Ensure workers is between 1 and CPU count."""
        cpu_count = os.cpu_count() or 1
        if self.workers < 1:
            raise ValueError("workers must be at least 1")
        if self.workers > cpu_count:
            self.workers = cpu_count
        return self

    @classmethod
    def from_yaml(cls, path: str | Path) -> Config:
        """Load configuration from a YAML file."""
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                return cls(**data)
        except Exception as e:
            raise ConfigError(f"Failed to load config from {path}: {e}") from e

    @classmethod
    def default(cls) -> Config:
        """Return a default configuration."""
        return cls()
