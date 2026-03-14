from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .config import Config

logger = logging.getLogger(__name__)


def extract_directory(
    dir_path: Path,
    parquet_files: list[Path],
    config: Config,
    root: Path,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """
    Extract audio and metadata from all Parquet files in a directory.

    Processes row groups one by one for memory efficiency.
    Returns a list of JSONL-ready records.
    """
    records: list[dict[str, Any]] = []

    # {parent_folder}_audio/
    dir_name = dir_path.name if dir_path != root else root.resolve().name
    audio_dir = dir_path / f"{dir_name}_audio"

    if not dry_run:
        os.makedirs(audio_dir, exist_ok=True)

    filename_counts: dict[str, int] = {}

    for parquet_path in parquet_files:
        logger.info(f"Processing {parquet_path.name}")

        try:
            parquet_file = pq.ParquetFile(parquet_path)
            num_row_groups = parquet_file.num_row_groups

            for i in range(num_row_groups):
                table = parquet_file.read_row_group(i)
                # table.to_pandas() requires pandas.
                try:
                    df_chunk = table.to_pandas()
                except ImportError:
                    logger.error(
                        "Pandas is required for extraction. "
                        "Install it with 'pip install pandas'."
                    )
                    return []

                for index, row in df_chunk.iterrows():
                    try:
                        # 1. Extract audio bytes
                        audio_struct = row.get(config.audio_column)

                        # Use filename_column or fallback
                        base_filename = str(row.get(config.filename_column))
                        if not base_filename or base_filename == "None":
                            base_filename = f"rg{i}_row{index}"

                        # Handle collisions
                        filename_counts[base_filename] = (
                            filename_counts.get(base_filename, 0) + 1
                        )
                        suffix = (
                            f"_{filename_counts[base_filename]-1}"
                            if filename_counts[base_filename] > 1
                            else ""
                        )

                        audio_filename = (
                            f"{base_filename}{suffix}.{config.audio_format}"
                        )
                        audio_save_path = audio_dir / audio_filename

                        # Relative path from root for JSONL
                        relative_audio_path = audio_save_path.relative_to(root)

                        # 2. Extract metadata
                        metadata = {
                            col: row.get(col) if col in row else None
                            for col in config.metadata_columns
                        }

                        # Record format
                        record = {
                            "audio_path": str(relative_audio_path),
                            "metadata": metadata,
                        }

                        # 3. Write audio if not dry run and (overwrite or not exists)
                        if not dry_run:
                            if config.overwrite or not audio_save_path.exists():
                                if (
                                    isinstance(audio_struct, dict)
                                    and "bytes" in audio_struct
                                ):
                                    with open(audio_save_path, "wb") as f:
                                        f.write(audio_struct["bytes"])
                                else:
                                    logger.warning(
                                        f"Missing audio field '{config.audio_column}' "
                                        f"at Row Group {i}, Row {index} in "
                                        f"{parquet_path.name}"
                                    )

                        records.append(record)

                    except Exception as e:
                        logger.warning(
                            f"Error processing Row Group {i}, Row {index} in "
                            f"{parquet_path.name}: {e}"
                        )

                del table
                del df_chunk
                logger.debug(f"Finished Row Group {i+1}/{num_row_groups}")

        except Exception as e:
            logger.error(f"Failed to read Parquet file {parquet_path}: {e}")

    return records
