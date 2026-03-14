from __future__ import annotations

from pathlib import Path

from vakya.walker import find_parquet_groups


def test_find_parquet_groups(tmp_path: Path) -> None:
    # Setup structure
    (tmp_path / "hindi").mkdir()
    (tmp_path / "tamil").mkdir()
    (tmp_path / ".hidden").mkdir()

    (tmp_path / "hindi" / "1.parquet").touch()
    (tmp_path / "hindi" / "2.parquet").touch()
    (tmp_path / "tamil" / "3.parquet").touch()
    (tmp_path / ".hidden" / "4.parquet").touch()
    (tmp_path / "empty").mkdir()

    groups = find_parquet_groups(tmp_path)

    assert len(groups) == 2
    assert (tmp_path / "hindi") in groups
    assert (tmp_path / "tamil") in groups
    assert (tmp_path / ".hidden") not in groups

    assert len(groups[tmp_path / "hindi"]) == 2
    assert len(groups[tmp_path / "tamil"]) == 1


def test_find_parquet_groups_root(tmp_path: Path) -> None:
    (tmp_path / "root.parquet").touch()

    groups = find_parquet_groups(tmp_path)
    assert len(groups) == 1
    assert tmp_path in groups
