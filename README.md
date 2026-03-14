# vakya

CLI tool to extract audio files and metadata from Parquet datasets into a structured, portable format.

## Installation

```bash
pip install vakya
```

## Quick Start

1. **Inspect a Parquet file** to see its columns:
   ```bash
   vakya schema data/train-00000.parquet
   ```

2. **Validate your config** (or use defaults):
   ```bash
   vakya validate-config my_config.yaml
   ```

3. **Extract everything**:
   ```bash
   vakya extract ./my_dataset --config my_config.yaml
   ```

## Configuration (YAML)

All behavior is controlled via a YAML config file.

```yaml
# vakya_config.yaml

# Column in the Parquet file that contains audio data (dict with "bytes").
audio_column: audio

# Output audio file extension.
audio_format: flac

# Column used for audio filename.
filename_column: fname

# List of columns to include in the metadata object.
metadata_columns:
  - fname
  - text
  - duration
  - gender
  - speaker_id
  - lang

# Name of the master JSONL file. Set to null to disable.
master_jsonl: master.jsonl

# Number of parallel workers (1 to CPU count).
workers: 1

# Whether to overwrite existing audio files.
overwrite: false

# Log level: DEBUG | INFO | WARNING | ERROR
log_level: INFO
```

## CLI Commands

- `vakya extract ROOT_DIR`: Recursively finds Parquet files and extracts them.
- `vakya validate-config CONFIG_PATH`: Checks if your YAML config is valid.
- `vakya schema PARQUET_FILE`: Prints the Arrow schema and row count of a Parquet file.

## Output Format

```
root_dataset/
├── hindi/
│   ├── train-00000.parquet
│   ├── hindi_audio/              ← Audio files here
│   │   ├── sample_001.flac
│   │   └── sample_002.flac
│   └── hindi.jsonl               ← Metadata for this folder
├── master.jsonl                  ← Aggregated metadata
└── config.yaml
```

Each line in the `.jsonl` files:

```json
{
  "audio_path": "hindi/hindi_audio/sample_001.flac",
  "metadata": {
    "fname": "sample_001",
    "text": "Hello world",
    "duration": 3.45,
    "gender": "female"
  }
}
```

## Development

Install development dependencies:
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

Run linting (Ruff):
```bash
ruff check src/ tests/
```

Run type checking (Mypy):
```bash
mypy src/ tests/
```

## CI/CD and Branching

- **Main Branch**: `master`
- **CI**: Every push/PR to `master` runs linting, type checking, and tests.
- **Publishing**: Automatic publishing to PyPI on version tags (`v*.*.*`) after tests pass. Requires GitHub environment approval.

## License

MIT
