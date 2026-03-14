"""
CLI command for inspecting data in a given directory.
"""
from pathlib import Path
from typing import List
import click

def get_all_subdirs(base_path: str = "./data") -> List[Path]:
    """Find subdirectories in a given path."""
    path = Path(base_path)
    if not path.exists():
        return []

    subdirs = sorted([p for p in path.rglob('*') if p.is_dir()])
    return subdirs

@click.group(name='inspect')
def inspect_group():
    """Inspect data in a given directory."""
    pass

@inspect_group.command(name='dirs')
@click.option(
    '--path', '-p',
    default='./data',
    help='Base path to inspect'
)
def inspect_dirs(path):
    """List all directories in a given path."""
    click.echo(f"Inspecting directories in {path}")

    directories = get_all_subdirs(path)

    if not directories:
        click.echo("No directories found.")
        return

    for d in directories:
        try:
            display_path = d.relative_to(path)
            click.echo(f"./{display_path}")
        except ValueError:
            click.echo(f"./{d}")