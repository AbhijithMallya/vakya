from __future__ import annotations

import os
from pathlib import Path


def find_parquet_groups(root: Path) -> dict[Path, list[Path]]:
    """
    Find Parquet files grouped by their parent directory.

    Returns a dict mapping each unique parent directory that contains
    at least one .parquet file to a list of .parquet file paths within it.
    """
    groups: dict[Path, list[Path]] = {}

    for dirpath, dirnames, filenames in os.walk(root):
        path = Path(dirpath)

        # Skip hidden directories (starting with .)
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        parquet_files = [path / f for f in filenames if f.endswith(".parquet")]
        if parquet_files:
            groups[path] = sorted(parquet_files)

    return groups
