from __future__ import annotations


class VakyaError(Exception):
    """Base exception for all vakya errors."""
    pass

class ConfigError(VakyaError):
    """Raised when there is an error in the configuration."""
    pass

class ExtractionError(VakyaError):
    """Raised when an error occurs during extraction."""
    pass

class AudioFieldError(ExtractionError):
    """Raised when the audio field is missing or invalid in a Parquet row."""
    pass
