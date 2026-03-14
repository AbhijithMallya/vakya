from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import pyarrow.parquet as pq
import yaml

from .config import Config
from .extractor import extract_directory
from .walker import find_parquet_groups
from .writer import write_jsonl, write_master_jsonl

# Set up root logger
logging.basicConfig(format="%(levelname)s: %(message)s")
logger = logging.getLogger("vakya")


@click.group()
def main() -> None:
    """Extract audio files and metadata from Parquet datasets."""
    pass


@main.command()
@click.argument(
    "root_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to YAML config file.",
)
@click.option("--workers", type=int, help="Override config workers setting.")
@click.option(
    "--overwrite/--no-overwrite",
    default=None,
    help="Override config overwrite setting.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print what would be done without writing any files.",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Set log level.",
)
@click.option("-v", "--verbose", is_flag=True, help="Shorthand for --log-level DEBUG.")
def extract(
    root_dir: Path,
    config_path: Path | None,
    workers: int | None,
    overwrite: bool | None,
    dry_run: bool,
    log_level: str,
    verbose: bool,
) -> None:
    """Extract audio and metadata from all Parquet files under ROOT_DIR."""
    # 1. Setup config and logging
    if verbose:
        log_level = "DEBUG"
    logger.setLevel(log_level)

    if config_path:
        config = Config.from_yaml(config_path)
    else:
        config = Config.default()

    if workers is not None:
        config.workers = workers
    if overwrite is not None:
        config.overwrite = overwrite

    start_time = time.time()

    # 2. Walk root_dir
    groups = find_parquet_groups(root_dir)
    total_parquet_files = sum(len(f) for f in groups.values())
    click.echo(
        f"Found {len(groups)} directories with {total_parquet_files} "
        "total Parquet files."
    )

    if dry_run:
        click.echo("Dry run enabled. No files will be written.")

    # 3. Process each directory
    per_dir_jsonl_paths: list[Path] = []
    total_extracted = 0

    def process_dir(dir_path: Path, parquet_files: list[Path]) -> tuple[int, Path]:
        # {parent_folder}.jsonl
        dir_name = dir_path.name if dir_path != root_dir else root_dir.resolve().name
        jsonl_path = dir_path / f"{dir_name}.jsonl"

        records = extract_directory(dir_path, parquet_files, config, root_dir, dry_run)

        if not dry_run and records:
            write_jsonl(records, jsonl_path)

        return len(records), jsonl_path

    if config.workers > 1:
        with ThreadPoolExecutor(max_workers=config.workers) as executor:
            futures = [executor.submit(process_dir, d, f) for d, f in groups.items()]
            for future in futures:
                count, jsonl_p = future.result()
                total_extracted += count
                per_dir_jsonl_paths.append(jsonl_p)
    else:
        for dir_path, files in groups.items():
            count, jsonl_p = process_dir(dir_path, files)
            total_extracted += count
            per_dir_jsonl_paths.append(jsonl_p)

    # 4. Write master JSONL
    if not dry_run and config.master_jsonl:
        master_path = root_dir / config.master_jsonl
        click.echo(
            f"Aggregating {len(per_dir_jsonl_paths)} files into {master_path}..."
        )
        write_master_jsonl(per_dir_jsonl_paths, master_path)

    elapsed = time.time() - start_time
    click.echo("\nFinal Summary:")
    click.echo(f"  Total records extracted: {total_extracted}")
    click.echo(f"  Total time elapsed: {elapsed:.2f}s")


@main.command()
@click.argument(
    "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def validate_config(config_path: Path) -> None:
    """Validate a YAML config file and print the parsed result."""
    try:
        config = Config.from_yaml(config_path)
        click.echo(yaml.dump(config.model_dump(), default_flow_style=False))
        click.echo("Config is valid.")
    except Exception as e:
        click.echo(f"Invalid config: {e}", err=True)
        sys.exit(1)


@main.command()
@click.argument(
    "parquet_file", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def schema(parquet_file: Path) -> None:
    """Inspect a Parquet file and print its schema."""
    try:
        pf = pq.ParquetFile(parquet_file)
        click.echo(f"File: {parquet_file}")
        click.echo(f"Row groups: {pf.num_row_groups}")
        click.echo(f"Total rows: {pf.metadata.num_rows}")
        click.echo("\nSchema:")
        click.echo(pf.schema_arrow)
    except Exception as e:
        click.echo(f"Error reading {parquet_file}: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
