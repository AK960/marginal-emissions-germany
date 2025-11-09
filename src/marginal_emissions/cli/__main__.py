"""CLI for fetching energy data from different APIs"""
import click
from pathlib import Path
from datetime import datetime
from typing import List

AVAILABLE_APIS = ['entsoe', 'smard', 'energycharts']

@click.group()
def cli():
    """Fetch energy data from endpoints and manage downloaded files"""
    pass

@cli.command('fetch')
@click.option(
    '--apis',
    '-a',
    type=click.Choice(AVAILABLE_APIS, case_sensitive=False),
    multiple=True,
    help='APIs to fetch data from. Can specify multiple APIs separated by space. Default: all APIs.'
)
@click.option(
    '--start',
    '-s',
    default='2024-01-01 00:00:00',
    help='Start date in YYYY-MM-DD format'
)
@click.option(
    '--end',
    '-e',
    default='2024-12-31 23:59:59',
    help='End date in YYYY-MM-DD format'
)
@click.option(
    '--output-dir',
    '-o',
    default='./data/raw',
    show_default=True,
    help='Base output directory where API subfolders will be created'
)
def fetch(apis: List[str], start: str, end: str, output_dir: str):
    """Fetch energy data from specified APIs"""
    # Wenn keine angegeben, alle benutzen
    selected_apis = list(apis) if apis else AVAILABLE_APIS

    click.echo(f"[GET] Curling [{', '.join(selected_apis)}] from {start} to {end}")

    for api in selected_apis:
        api_output_dir = Path(output_dir) / api
        api_output_dir.mkdir(parents=True, exist_ok=True)


        # TODO: Implement fetching logic + printing api_output_dir in that function
        if api == 'entsoe':
            click.echo(f"[GET] Requesting '{api}' ...")
            # fetch_entsoe_data(start, end, api_output_dir)
            click.echo(f"[SAVE] Saving to '{api_output_dir}'")
            click.echo(f"// Would fetch ENTSO-E data from {start} to {end}")
        elif api == 'smard':
            click.echo(f"[GET] Requesting '{api}' ...")
            # fetch_smard_data(start, end, api_output_dir)
            click.echo(f"[SAVE] Saving to '{api_output_dir}'")
            click.echo(f"// Would fetch SMARD data from {start} to {end}")
        elif api == 'energycharts':
            click.echo(f"[GET] Requesting '{api}' ...")
            # fetch_energycharts_data(start, end, api_output_dir)
            click.echo(f"[SAVE] Saving to '{api_output_dir}'")
            click.echo(f"// Would fetch EnergyCharts data from {start} to {end}")

    click.echo(f"\nData fetching completed!")

@cli.command('listfiles')
@click.option(
    '--raw',
    '-r',
    is_flag=True,
    help='Show only files in raw data directory (./data/raw/)'
)
@click.option(
    '--source',
    '-s',
    type=click.Choice(AVAILABLE_APIS, case_sensitive=False),
    help='Filter by specific API source (only works with --raw)'
)
@click.option(
    '--data-dir',
    '-d',
    default='./data',
    show_default=True,
    help='Base data directory to scan (only used without --raw)'
)
def listfiles(raw: bool, source: str, data_dir: str):
    """List downloaded files with metadata

    Without options: shows everything under ./data/
    With --raw: shows only ./data/raw/
    With --raw --source <api>: shows only ./data/raw/<api>/
    """
    if raw:
        base_dir = Path('./data/raw/')
        if source:
            target_dir = base_dir / source
            dir_description = f".data/raw/{source}/"
            show_subdirs = False
        else:
            target_dir = base_dir
            dir_description = "./data/raw/"
            show_subdirs = True
    else:
        target_dir = Path(data_dir)
        dir_description = data_dir
        show_subdirs = True

    if not target_dir.exists():
        click.echo(f"Directory does not exist: {target_dir}")
        return

    click.echo(f"### Files in {dir_description}: ###")

    if show_subdirs:
        # Rekursives Listing für Ordner
        _list_directory_contents_recursive(target_dir)
    else:
        # Nicht-rekursives Listing nur für Dateien im aktuellen Ordner
        _list_files_in_directory(target_dir)


def _list_directory_contents_recursive(directory: Path, current_depth: int = 0):
    """Recursively list all directory contents."""
    if not directory.exists():
        return

    # Alle Items im aktuellen Verzeichnis sortieren
    items = sorted(directory.iterdir())

    for item in items:
        indent = "  " * current_depth

        if item.is_dir():
            # Ordner anzeigen und rekursiv durchsuchen
            click.echo(f"{indent} {item.name}/")
            _list_directory_contents_recursive(item, current_depth + 1)
        else:
            # Datei anzeigen
            _print_file_info(item, directory, current_depth)

def _list_files_in_directory(directory: Path):
    """List files in a single directory."""
    files = [f for f in sorted(directory.iterdir()) if f.is_file()]

    for file_path in files:
        _print_file_info(file_path, directory)


def _print_file_info(file_path: Path, base_dir: Path = None, depth: int = 0):
    """Print file information with formatting."""
    stat = file_path.stat()
    size_kb = stat.st_size / 1024
    mod_time = datetime.fromtimestamp(stat.st_mtime)
    indent = "  " * depth

    if base_dir:
        try:
            display_path = str(file_path.relative_to(base_dir))
        except ValueError:
            display_path = str(file_path.name)
    else:
        display_path = str(file_path.name)

    # JSON-String Ausgabe mit Indent
    click.echo(f'{indent}- {{"file": "{display_path}", "size": {size_kb:.1f}, "modified": "{mod_time.strftime("%Y-%m-%d %H:%M:%S")}"}}')

@cli.command('apilist')
def apilist():
    """Show available APIs that can be used with the fetch command."""
    click.echo(f"Available APIs: {AVAILABLE_APIS}")

def main():
    """Entry point for CLI tool"""
    cli()

if __name__ == '__main__':
    main()