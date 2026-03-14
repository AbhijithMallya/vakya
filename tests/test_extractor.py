from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vakya.config import Config
from vakya.extractor import extract_directory


def test_extract_directory(tmp_path: Path, synthetic_parquet: Any) -> None:
    dir_path = tmp_path / "hindi"
    dir_path.mkdir()
    parquet_path = dir_path / "test.parquet"
    synthetic_parquet(parquet_path, rows=10, row_group_size=5)

    config = Config.default()

    records = extract_directory(dir_path, [parquet_path], config, tmp_path)

    assert len(records) == 10

    # Check if files were created
    audio_dir = dir_path / "hindi_audio"
    assert audio_dir.exists()
    assert len(list(audio_dir.glob("*.flac"))) == 10

    # Check relative path in record
    first_record = records[0]
    expected_path = str(Path("hindi") / "hindi_audio" / "sample_0.flac")
    assert first_record["audio_path"] == expected_path

    # Check metadata
    assert first_record["metadata"]["text"] == "text_0"


def test_extract_directory_dry_run(tmp_path: Path, synthetic_parquet: Any) -> None:
    dir_path = tmp_path / "tamil"
    dir_path.mkdir()
    parquet_path = dir_path / "test.parquet"
    synthetic_parquet(parquet_path, rows=5)

    config = Config.default()

    records = extract_directory(
        dir_path, [parquet_path], config, tmp_path, dry_run=True
    )

    assert len(records) == 5
    audio_dir = dir_path / "tamil_audio"
    assert not audio_dir.exists()


def test_extract_directory_filename_collision(tmp_path: Path) -> None:
    dir_path = tmp_path / "collision"
    dir_path.mkdir()
    parquet_path = dir_path / "test.parquet"

    # Create two rows that will collide on fname if not handled
    data = [
        {"audio": {"bytes": b"b1"}, "fname": "same"},
        {"audio": {"bytes": b"b2"}, "fname": "same"},
    ]
    df = pd.DataFrame(data)
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    config = Config.default()
    records = extract_directory(dir_path, [parquet_path], config, tmp_path)

    audio_dir = dir_path / "collision_audio"
    assert (audio_dir / "same.flac").exists()
    assert (audio_dir / "same_1.flac").exists()
    assert len(records) == 2
