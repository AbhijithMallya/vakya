from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def write_jsonl(records: list[dict[str, Any]], path: Path) -> int:
    """
    Write records to a JSONL file atomically.

    Returns the number of records written.
    """
    if not records:
        return 0

    temp_path = path.with_suffix(".tmp")
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        os.replace(temp_path, path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    return len(records)


def write_master_jsonl(per_dir_jsonl_paths: list[Path], master_path: Path) -> int:
    """
    Stream per-directory JSONLs into one master file atomically.

    Returns total count of records written.
    """
    total_count = 0
    temp_path = master_path.with_suffix(".tmp")

    try:
        with open(temp_path, "w", encoding="utf-8") as out_f:
            for jsonl_path in per_dir_jsonl_paths:
                if not jsonl_path.exists():
                    continue
                with open(jsonl_path, encoding="utf-8") as in_f:
                    for line in in_f:
                        out_f.write(line)
                        total_count += 1
        os.replace(temp_path, master_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    return total_count
