from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    return tmp_path

def make_audio_bytes(content: bytes = b"FAKEAUDIO") -> bytes:
    """Return minimal valid-ish audio bytes."""
    return content + os.urandom(16)

def build_parquet(
    path: Path,
    rows: int = 10,
    row_group_size: int = 5,
    audio_col: str = "audio",
    fname_col: str = "fname",
    metadata_cols: list[str] | None = None
) -> None:
    """Write a synthetic Parquet file with audio bytes + metadata columns."""
    if metadata_cols is None:
        metadata_cols = ["text", "duration", "gender", "speaker_id", "lang"]

    data = []
    for i in range(rows):
        row = {
            audio_col: {"bytes": make_audio_bytes(f"audio_{i}".encode())},
            fname_col: f"sample_{i}",
        }
        for col in metadata_cols:
            row[col] = f"{col}_{i}"
        data.append(row)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)

    pq.write_table(table, path, row_group_size=row_group_size)

@pytest.fixture
def synthetic_parquet() -> Callable[..., None]:
    return build_parquet
