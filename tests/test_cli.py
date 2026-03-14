from __future__ import annotations

from pathlib import Path
from typing import Any

from click.testing import CliRunner

from vakya.cli import main


def test_cli_validate_config(tmp_path: Path) -> None:
    runner = CliRunner()
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        f.write("audio_column: audio_data\n")

    result = runner.invoke(main, ["validate-config", str(config_file)])
    assert result.exit_code == 0
    assert "audio_column: audio_data" in result.output

def test_cli_schema(tmp_path: Path, synthetic_parquet: Any) -> None:
    runner = CliRunner()
    parquet_path = tmp_path / "test.parquet"
    synthetic_parquet(parquet_path, rows=10)

    result = runner.invoke(main, ["schema", str(parquet_path)])
    assert result.exit_code == 0
    assert "Total rows: 10" in result.output

def test_cli_extract(tmp_path: Path, synthetic_parquet: Any) -> None:
    runner = CliRunner()

    # Setup data
    hindi_dir = tmp_path / "hindi"
    hindi_dir.mkdir()
    synthetic_parquet(hindi_dir / "h1.parquet", rows=5)

    result = runner.invoke(main, ["extract", str(tmp_path)])
    assert result.exit_code == 0
    assert "Total records extracted: 5" in result.output

    # Check outputs
    assert (hindi_dir / "hindi.jsonl").exists()
    assert (hindi_dir / "hindi_audio").exists()
    assert (tmp_path / "master.jsonl").exists()

def test_cli_extract_dry_run(tmp_path: Path, synthetic_parquet: Any) -> None:
    runner = CliRunner()
    hindi_dir = tmp_path / "hindi"
    hindi_dir.mkdir()
    synthetic_parquet(hindi_dir / "h1.parquet", rows=5)

    result = runner.invoke(main, ["extract", "--dry-run", str(tmp_path)])
    assert result.exit_code == 0
    assert "Dry run enabled" in result.output
    assert not (hindi_dir / "hindi_audio").exists()
