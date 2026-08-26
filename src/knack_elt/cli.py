import logging
import os
from pathlib import Path

import dlt
import typer
from knack_sleuth import load_app_metadata
from rich.console import Console
from rich.table import Table

from knack_elt import __version__
from knack_elt.config import settings
from knack_elt.knack_dlt import build_knack_resources, create_rest_client

# pretty_exceptions_show_locals is already off by default in Typer >= 0.17, but the
# pin is a floor: an unhandled exception must never print the API key or the
# token-bearing MotherDuck connection string, so state it rather than inherit it.
cli = typer.Typer(pretty_exceptions_show_locals=False)
console = Console()

logging.basicConfig(level=logging.INFO)

def default_db_dir() -> Path:
    """Where local DuckDB files go when --db-path is not given.

    A stable per-user directory, not the working directory: the same app must
    keep one warehouse wherever the command is run. A CWD-relative default
    silently splits a record's SCD2 history across directories, which is worse
    than being one `--db-path` away from the location you wanted.
    """
    xdg = os.environ.get("XDG_DATA_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "share"
    return base / "knack-elt"


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
        help="Knack application ID to extract data from. Defaults to $KNACK_APP_ID."
    ),
    api_key: str = typer.Option(
        None,
        "--api-key",
        help="Knack REST API key. Defaults to $KNACK_API_KEY.",
    ),
    destination: str = typer.Option(
        "local",
        "--destination",
        "-d",
        help="Where to load: 'local' (a DuckDB file, no account needed) or 'motherduck'.",
    ),
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Local DuckDB file. Defaults to $XDG_DATA_HOME (or ~/.local/share)"
             "/knack-elt/knack_{slug}_data.duckdb.",
    ),
    refresh_metadata: bool = typer.Option(
        False,
        "--refresh-metadata",
        help="Re-fetch app metadata instead of using knack-sleuth's 24h on-disk cache.",
    ),
    skip_unreadable: bool = typer.Option(
        False,
        "--skip-unreadable",
        help="Log and skip objects that fail before yielding any row (e.g. no read "
             "permission) instead of aborting the run. An object that fails partway "
             "through still aborts: a partial batch would retire live SCD2 rows.",
    ),
):
    """Run the ELT pipeline for a Knack application."""
    final_app_id = app_id or settings.knack_app_id
    final_api_key = api_key or settings.knack_api_key

    if not final_app_id:
        console.print("[bold red]Error:[/bold red] app_id is required. Provide it via --app-id option or set KNACK_APP_ID environment variable.")
        raise typer.Exit(code=1)

    if not final_api_key:
        console.print("[bold red]Error:[/bold red] a Knack REST API key is required to read records. Provide it via --api-key or set KNACK_API_KEY.")
        raise typer.Exit(code=1)

    if destination not in ("local", "motherduck"):
        console.print(f"[bold red]Error:[/bold red] unknown destination {destination!r}; expected 'local' or 'motherduck'.")
        raise typer.Exit(code=1)

    if destination == "motherduck" and db_path is not None:
        console.print("[bold red]Error:[/bold red] --db-path only applies to --destination local.")
        raise typer.Exit(code=1)

    if destination == "motherduck" and not settings.motherduck_api_key:
        console.print("[bold red]Error:[/bold red] --destination motherduck requires motherduck_api_key in the environment or .env.")
        raise typer.Exit(code=1)

    kn_app = load_app_metadata(app_id=final_app_id, refresh=refresh_metadata).application

    # Slugs commonly contain dashes; identifiers downstream should not.
    slug = kn_app.slug.replace('-', '_')
    dest_db_name = f"knack_{slug}_data"
    dlt_pipeline_name = f"knack_{slug}_pipeline"
    dataset_name = slug

    if destination == "local":
        local_db_path = (db_path or default_db_dir() / f"{dest_db_name}.duckdb").resolve()
        local_db_path.parent.mkdir(parents=True, exist_ok=True)
        dlt_destination = dlt.destinations.duckdb(str(local_db_path))
        destination_label = str(local_db_path)
    else:
        local_db_path = None
        dlt_destination = dlt.destinations.motherduck(
            f"md:///{dest_db_name}?token={settings.motherduck_api_key}"
        )
        destination_label = f"MotherDuck md:///{dest_db_name}"

    summary = Table(show_header=False, box=None)
    summary.add_column(style="bold")
    summary.add_column(style="cyan bold")
    summary.add_row("App", f"{kn_app.name} ({final_app_id})")
    summary.add_row("Slug", kn_app.slug)
    summary.add_row("Objects", str(len(kn_app.objects)))
    summary.add_row("Destination", destination_label)
    summary.add_row("Dataset", dataset_name)
    summary.add_row("dlt pipeline", dlt_pipeline_name)
    console.print(summary)

    dlt.config["load.workers"] = 3
    dlt.config["truncate_staging_dataset"] = True

    knack_dlt_pipeline = dlt.pipeline(
        pipeline_name=dlt_pipeline_name,
        dataset_name=dataset_name,
        dev_mode=False,
        destination=dlt_destination,
    )

    client = create_rest_client(app_id=final_app_id, api_key=final_api_key)
    load_info = knack_dlt_pipeline.run(
        build_knack_resources(kn_app, client, skip_unreadable=skip_unreadable)
    )

    console.print(load_info)
    console.print(f"Elapsed: {(load_info.finished_at - load_info.started_at).in_words()}")

    # Keep the run's own bookkeeping alongside the data.
    knack_dlt_pipeline.run([load_info], table_name="_load_info")
    knack_dlt_pipeline.run([knack_dlt_pipeline.last_trace], table_name="_trace")


if __name__ == "__main__":
    cli()
