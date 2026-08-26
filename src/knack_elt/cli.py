import logging
import os
import re
from hashlib import sha256
from pathlib import Path

import dlt
import typer
from knack_sleuth import load_app_metadata
from rich.console import Console
from rich.table import Table

from knack_elt import __version__
from knack_elt.config import settings
from knack_elt.knack_dlt import (
    build_knack_resources,
    create_rest_client,
    reconcile_scd2_tables,
)

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


def stable_app_identifier(app_id: str) -> str:
    """A filesystem/SQL-safe identity derived from the immutable Knack app id."""
    readable = re.sub(r"[^a-z0-9]+", "_", app_id.lower()).strip("_")[:32] or "app"
    digest = sha256(app_id.encode()).hexdigest()[:8]
    return f"app_{readable}_{digest}"


def legacy_db_path(slug: str, directory: Path) -> Path:
    """Where releases before stable identities put this app's local warehouse.

    Naming moved from the editable slug to `stable_app_identifier()`, which starts a
    new physical namespace on purpose - the old file is not migrated, and nothing in
    the new pipeline can see it. Point at it so an upgrade does not look like data
    loss.
    """
    return directory / f"knack_{slug.replace('-', '_')}_data.duckdb"


def schema_catalog_rows(kn_app):
    """Current human-readable labels for the stable physical object/field keys."""
    objects = [
        {"object_id": obj.key, "object_name": obj.name, "table_name": obj.key}
        for obj in kn_app.objects
    ]
    fields = [
        {
            "object_id": obj.key,
            "object_name": obj.name,
            "field_key": field.key,
            "field_name": field.name,
            "column_name": field.key,
            "field_type": field.type,
        }
        for obj in kn_app.objects
        for field in obj.fields
    ]
    return objects, fields


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
             "/knack-elt/knack_{stable_app_id}_data.duckdb.",
    ),
    refresh_metadata: bool = typer.Option(
        False,
        "--refresh-metadata",
        help="Re-fetch app metadata instead of using knack-sleuth's 24h on-disk cache.",
    ),
    skip_unreadable: bool = typer.Option(
        False,
        "--skip-unreadable",
        help="Log and skip an object that returns HTTP 403 before yielding any row "
             "(no read permission on the API key) instead of aborting the run. Nothing "
             "else is swallowed: 401, 429, 5xx, timeouts and network errors all abort, "
             "as does a short batch. An object that fails partway "
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

    # App slugs are editable display metadata. All physical identities derive from
    # the immutable application id so a URL rename cannot split SCD2 history.
    app_identifier = stable_app_identifier(final_app_id)
    dest_db_name = f"knack_{app_identifier}_data"
    dlt_pipeline_name = f"knack_{app_identifier}_pipeline"
    dataset_name = app_identifier

    if destination == "local":
        local_db_path = (db_path or default_db_dir() / f"{dest_db_name}.duckdb").resolve()
        local_db_path.parent.mkdir(parents=True, exist_ok=True)
        dlt_destination = dlt.destinations.duckdb(str(local_db_path))
        destination_label = str(local_db_path)
        legacy_path = legacy_db_path(kn_app.slug, local_db_path.parent)
        if legacy_path != local_db_path and legacy_path.exists():
            console.print(
                f"[bold yellow]Note:[/bold yellow] a slug-named warehouse from an earlier "
                f"release is still at {legacy_path}. Physical names now derive from the "
                f"immutable app id, so this run starts a fresh history at {local_db_path} "
                f"and leaves the old file untouched."
            )
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
    summary.add_row("Stable ID", app_identifier)
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
    extraction_status = {}
    load_info = knack_dlt_pipeline.run(
        build_knack_resources(
            kn_app,
            client,
            skip_unreadable=skip_unreadable,
            extraction_status=extraction_status,
        )
    )
    data_trace = knack_dlt_pipeline.last_trace

    reconcile_scd2_tables(knack_dlt_pipeline, kn_app, extraction_status)

    console.print(load_info)
    console.print(f"Elapsed: {(load_info.finished_at - load_info.started_at).in_words()}")

    # Physical tables/columns use immutable keys. Keep the current labels beside
    # them so analysts can discover the schema without making labels identifiers.
    object_catalog, field_catalog = schema_catalog_rows(kn_app)
    if object_catalog:
        knack_dlt_pipeline.run(
            object_catalog, table_name="_kn_object_catalog", write_disposition="replace"
        )
    elif "_kn_object_catalog" in knack_dlt_pipeline.default_schema.tables:
        with knack_dlt_pipeline.sql_client() as sql_client:
            sql_client.truncate_tables("_kn_object_catalog")
    if field_catalog:
        knack_dlt_pipeline.run(
            field_catalog, table_name="_kn_field_catalog", write_disposition="replace"
        )
    elif "_kn_field_catalog" in knack_dlt_pipeline.default_schema.tables:
        with knack_dlt_pipeline.sql_client() as sql_client:
            sql_client.truncate_tables("_kn_field_catalog")

    # Keep the data run's own bookkeeping alongside the data. Capture the trace
    # before loading these bookkeeping rows so `_trace` describes the actual sync.
    #
    # Dispositions are stated rather than inherited, because the two differ. A
    # load_info row is one row per sync and is the audit trail you query for when the
    # warehouse last moved, so it appends. A trace normalizes into 19 tables, several
    # of which grow per run - `_trace__steps__step_info__load_packages__tables__columns`
    # is one row per column per table per sync - so keeping every historical trace
    # costs more than it is worth. Only the last run's detail is kept.
    knack_dlt_pipeline.run([load_info], table_name="_load_info", write_disposition="append")
    knack_dlt_pipeline.run([data_trace], table_name="_trace", write_disposition="replace")


if __name__ == "__main__":
    cli()
