from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from vakya.writer import write_jsonl, write_master_jsonl


def test_write_jsonl(tmp_path: Path) -> None:
    records: list[dict[str, Any]] = [
        {"audio_path": "a1.flac", "metadata": {}},
        {"audio_path": "a2.flac", "metadata": {}},
    ]
    path = tmp_path / "test.jsonl"
    count = write_jsonl(records, path)

    assert count == 2
    assert path.exists()

    with open(path) as f:
        lines = f.readlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["audio_path"] == "a1.flac"


def test_write_master_jsonl(tmp_path: Path) -> None:
    d1 = tmp_path / "d1"
    d1.mkdir()
    f1 = d1 / "d1.jsonl"
    with open(f1, "w") as f:
        f.write('{"r": 1}\n')

    d2 = tmp_path / "d2"
    d2.mkdir()
    f2 = d2 / "d2.jsonl"
    with open(f2, "w") as f:
        f.write('{"r": 2}\n')

    master = tmp_path / "master.jsonl"
    count = write_master_jsonl([f1, f2], master)

    assert count == 2
    assert master.exists()

    with open(master) as f:
        lines = f.readlines()
        assert len(lines) == 2
