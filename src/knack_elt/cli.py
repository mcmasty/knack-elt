from knack_elt import __version__
from knack_elt.config import settings

from pathlib import Path

from knack_sleuth import load_app_metadata

import dlt

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
    
from knack_elt.knack_dlt import build_knack_resources


cli = typer.Typer()
console = Console()

def version_callback(value: bool):
    """Display version and exit."""
    if value:
        console.print(f"knack-elt version {__version__}")
        raise typer.Exit()

@cli.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit."
    )
):
    pass


@cli.command()
def run_pipeline(
    app_id: str = typer.Option(
        None,
        "--app-id",
        help="Knack application ID to extract data from"
    )
):
    """Run the ELT pipeline for a Knack application."""
    final_app_id = app_id or settings.knack_app_id
    
    if not final_app_id:
        console.print("[bold red]Error:[/bold red] app_id is required. Provide it via --app-id option or set KNACK_APP_ID environment variable.")
        raise typer.Exit(code=1)
        
    kn_app = load_app_metadata(app_id=final_app_id).application
    
    dest_db_name = f"knack_{kn_app.slug}_data"
    dlt_pipeline_name = f"knack_{kn_app.slug}_pipeline"
    dataset_name = f"{kn_app.slug.replace('-', '_')}"
    console.print(f"Running Pipeline for [red bold]{final_app_id}[/red bold]")
    console.print(f"Running Pipeline for [cyan bold]{kn_app.name}[/cyan bold]")
    console.print(f"slug               : [cyan bold]{kn_app.slug}[/cyan bold]")    
    console.print(f"Destination DB Name: [cyan bold]{dest_db_name}[/cyan bold]")    
    console.print(f"Dataset Name       : [cyan bold]{dataset_name}[/cyan bold]")        
    console.print(f"DLT Pipeline Name  : [cyan bold]{dlt_pipeline_name}[/cyan bold]")    

    # TODO: flag for dev/test/local vs motherduck...
    #   or some other control around destination....
    
    local_duckdb_filename = f"tests/data/{dest_db_name}.duckdb"
    # Ensure the parent directory exists
    file_path = Path(local_duckdb_filename)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    knack_dlt_pipeline = dlt.pipeline(
        pipeline_name=dlt_pipeline_name,
        dataset_name=dataset_name,
        dev_mode=False,
        destination=dlt.destinations.duckdb(local_duckdb_filename),

        # destination=dlt.destinations.motherduck(
        #     f"md:///{dest_db_name}?token={settings.motherduck_api_key}"
        # ),
        # export_schema_path="pipelines/schemas/export",
        # import_schema_path="pipelines/schemas/import",
    )
    load_info = knack_dlt_pipeline.run(build_knack_resources(kn_app))
    
    console.print(load_info)
    
if __name__ == "__main__":
    cli()