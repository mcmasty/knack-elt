import logging
import os
import re
import sys
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
from knack_elt.labels import (
    LabelNameCollision,
    MissingCatalogs,
    apply_label_views,
    labels_schema_name,
    plan_label_views,
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


_WAREHOUSE_NAME = re.compile(r"[a-z][a-z0-9_]{0,39}$")
_DERIVED_SHAPE = re.compile(r"app_.*_[0-9a-f]{8}$")


def resolve_warehouse_name(flag_value: str | None) -> str | None:
    """--name beats KNACK_WAREHOUSE_NAME beats None (derive from the app id).

    An explicitly passed empty --name is returned as-is so validation rejects it;
    silently treating it as "unset" would make a quoting mistake in a wrapper
    script fall back to the derived name and split the warehouse.
    """
    if flag_value is not None:
        return flag_value
    return settings.warehouse_name or None


def validate_warehouse_name(name: str) -> str | None:
    """The reason a name is unusable, or None.

    Validated, never transformed: DuckDB folds identifiers case-insensitively and
    dlt normalizes names, so a name that silently becomes something else is how the
    same app ends up with two warehouses. Requiring the already-folded form up
    front means what you typed is what every layer uses.
    """
    if not _WAREHOUSE_NAME.match(name):
        return (
            f"invalid warehouse name {name!r}: must start with a lowercase letter and "
            f"contain only lowercase letters, digits and underscores (max 40 chars)."
        )
    if _DERIVED_SHAPE.match(name):
        return (
            f"invalid warehouse name {name!r}: it has the shape of a derived "
            f"identifier (app_*_<8 hex>), which risks colliding with a real one."
        )
    return None


def warn_on_sibling_warehouse(app_id: str, name: str | None, directory: Path) -> str | None:
    """A warning when this app's *other* naming also has a warehouse on disk.

    --name today and no flag tomorrow silently splits SCD2 history across two
    files - the same failure a CWD-relative default had. Warn, never block: both
    warehouses are legitimate, and which one is stale is the operator's call.
    """
    derived = stable_app_identifier(app_id)
    if name is not None and name != derived:
        sibling, label = derived, "derived from the app id"
    elif name is None:
        candidates = [
            path for path in directory.glob("knack_*_data.duckdb")
            if not _DERIVED_SHAPE.match(path.name[len("knack_"):-len("_data.duckdb")])
        ]
        if not candidates:
            return None
        sibling_path = candidates[0]
        return (
            f"a named warehouse also exists at {sibling_path}. This run uses the "
            f"derived name {derived!r}; if this app usually runs with --name (or "
            f"KNACK_WAREHOUSE_NAME), running without it splits SCD2 history "
            f"across two files."
        )
    else:
        return None
    sibling_file = directory / f"knack_{sibling}_data.duckdb"
    if sibling_file.exists():
        return (
            f"a warehouse {label} also exists at {sibling_file}. This run uses "
            f"--name {name!r}; mixing the two naming modes splits SCD2 history "
            f"across two files."
        )
    return None


def resolve_destination(app_id: str, destination: str, db_path: Path | None,
                        name: str | None = None):
    """The dlt destination, dataset, pipeline name and warehouse identifiers both
    commands share.

    Extracted so `refresh-views` cannot drift from `run-pipeline`: two commands
    deriving the same warehouse two different ways is how a rename ends up applied
    to the wrong file. Callers must validate `--destination motherduck` needs
    `motherduck_api_key` *before* calling this - the token is embedded in the
    connection string built here.
    """
    app_identifier = name or stable_app_identifier(app_id)
    dest_db_name = f"knack_{app_identifier}_data"
    dlt_pipeline_name = f"knack_{app_identifier}_pipeline"
    dataset_name = app_identifier

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

    return (
        dlt_destination, app_identifier, dataset_name, dlt_pipeline_name,
        local_db_path, destination_label,
    )


def _stdin_is_tty() -> bool:
    """A seam: Typer's CliRunner substitutes its own stdin stream during `invoke`,
    so tests patch this function rather than `sys.stdin.isatty` directly."""
    return sys.stdin.isatty()


def _report_label_drift(pipeline) -> None:
    """Read-only, and this must never fail or change `run-pipeline`'s exit code -
    a successful load that cannot be described is still a successful load.

    Silent when there is no drift, and silent when the `_labels` schema does not
    exist at all: a warehouse that has never had `refresh-views` run against it is
    not "drifted", it is simply a user who has not opted into the view layer. Only
    `plan_label_views` (via `refresh-views`) treats "nothing built yet" as a plan to
    create everything; this report must not spam that same warehouse on every sync.
    """
    try:
        labels_schema = labels_schema_name(pipeline.dataset_name)
        with pipeline.sql_client() as sql_client:
            exists = bool(sql_client.execute_sql(
                "SELECT 1 FROM information_schema.schemata "
                "WHERE catalog_name = current_database() AND schema_name = %s",
                labels_schema,
            ))
        if not exists:
            return

        drift = plan_label_views(pipeline)
        if drift.is_empty():
            return

        console.print(f"\n[bold yellow]Label drift[/bold yellow] in {drift.labels_schema}:")
        for old, new, object_key in drift.renamed:
            console.print(f"  ~ {old!r} -> {new!r}  ({object_key})")
        for name, reason in drift.changed:
            console.print(f"  * {name!r} {reason}")
        for name in drift.created:
            console.print(f"  + {name!r}")
        for name in drift.dropped:
            console.print(f"  - {name!r}")
        console.print("Run `knack-elt refresh-views` to update the view layer.")
    except MissingCatalogs:
        pass
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not check label drift: {e}")


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
    name: str = typer.Option(
        None,
        "--name",
        help="Warehouse name override: pins the db file, dataset, pipeline and "
             "labels schema to knack_{name}_data / {name} / {name}_labels instead "
             "of deriving them from the app id. Defaults to $KNACK_WAREHOUSE_NAME. "
             "Once chosen, pass it on every run - mixing named and derived runs "
             "splits SCD2 history across two warehouses.",
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
    final_name = resolve_warehouse_name(name)

    if not final_app_id:
        console.print("[bold red]Error:[/bold red] app_id is required. Provide it via --app-id option or set KNACK_APP_ID environment variable.")
        raise typer.Exit(code=1)

    if not final_api_key:
        console.print("[bold red]Error:[/bold red] a Knack REST API key is required to read records. Provide it via --api-key or set KNACK_API_KEY.")
        raise typer.Exit(code=1)

    if final_name is not None and (reason := validate_warehouse_name(final_name)):
        console.print(f"[bold red]Error:[/bold red] {reason}")
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
    dlt_destination, app_identifier, dataset_name, dlt_pipeline_name, local_db_path, \
        destination_label = resolve_destination(final_app_id, destination, db_path, name=final_name)

    if destination == "local":
        legacy_path = legacy_db_path(kn_app.slug, local_db_path.parent)
        if legacy_path != local_db_path and legacy_path.exists():
            console.print(
                f"[bold yellow]Note:[/bold yellow] a slug-named warehouse from an earlier "
                f"release is still at {legacy_path}. Physical names now derive from the "
                f"immutable app id, so this run starts a fresh history at {local_db_path} "
                f"and leaves the old file untouched."
            )
        sibling = warn_on_sibling_warehouse(final_app_id, final_name, local_db_path.parent)
        if sibling:
            console.print(f"[bold yellow]Note:[/bold yellow] {sibling}")

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

    # Detected, not prevented. A label edit in the Knack builder must never move a
    # warehouse name on its own - a dashboard should break when someone chose it to,
    # not because a form was edited on a Tuesday. This can never change the exit
    # code: a successful load that cannot be described is still a successful load.
    _report_label_drift(knack_dlt_pipeline)


@cli.command()
def refresh_views(
    app_id: str = typer.Option(
        None,
        "--app-id",
        help="Knack application ID. Defaults to $KNACK_APP_ID.",
    ),
    destination: str = typer.Option(
        "local",
        "--destination",
        "-d",
        help="Where the warehouse lives: 'local' or 'motherduck'.",
    ),
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Local DuckDB file. Same default as run-pipeline.",
    ),
    name: str = typer.Option(
        None,
        "--name",
        help="Warehouse name override: pins the db file, dataset, pipeline and "
             "labels schema to knack_{name}_data / {name} / {name}_labels instead "
             "of deriving them from the app id. Defaults to $KNACK_WAREHOUSE_NAME. "
             "Once chosen, pass it on every run - mixing named and derived runs "
             "splits SCD2 history across two warehouses.",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Apply without confirming. Required for non-interactive use.",
    ),
):
    """Rebuild the label views to match the labels from the last sync.

    Reads the catalogs already in the warehouse - no Knack API key, no network. The
    views reflect labels as of the last sync, not as of right now.
    """
    final_app_id = app_id or settings.knack_app_id
    final_name = resolve_warehouse_name(name)
    if not final_app_id:
        console.print(
            "[bold red]Error:[/bold red] app_id is required. Provide it via "
            "--app-id option or set KNACK_APP_ID environment variable."
        )
        raise typer.Exit(code=1)

    if final_name is not None and (reason := validate_warehouse_name(final_name)):
        console.print(f"[bold red]Error:[/bold red] {reason}")
        raise typer.Exit(code=1)

    if destination not in ("local", "motherduck"):
        console.print(
            f"[bold red]Error:[/bold red] unknown destination {destination!r}; "
            f"expected 'local' or 'motherduck'."
        )
        raise typer.Exit(code=1)

    if destination == "motherduck" and db_path is not None:
        console.print(
            "[bold red]Error:[/bold red] --db-path only applies to --destination local."
        )
        raise typer.Exit(code=1)

    if destination == "motherduck" and not settings.motherduck_api_key:
        console.print(
            "[bold red]Error:[/bold red] --destination motherduck requires "
            "motherduck_api_key in the environment or .env."
        )
        raise typer.Exit(code=1)

    dlt_destination, _app_identifier, dataset_name, dlt_pipeline_name, _local_db_path, \
        destination_label = resolve_destination(final_app_id, destination, db_path, name=final_name)

    pipeline = dlt.pipeline(
        pipeline_name=dlt_pipeline_name,
        dataset_name=dataset_name,
        dev_mode=False,
        destination=dlt_destination,
    )

    try:
        plan = plan_label_views(pipeline)
    except (MissingCatalogs, LabelNameCollision) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from e

    console.print(f"Plan for {plan.labels_schema} ({destination_label}):")
    for old, new, object_key in plan.renamed:
        console.print(f"  ~ {old!r} -> {new!r}  ({object_key})")
    for name, reason in plan.changed:
        console.print(f"  * {name!r} {reason}")
    for name in plan.created:
        console.print(f"  + {name!r}")
    for name in plan.dropped:
        console.print(f"  - {name!r}")
    for object_key, reason in plan.skipped:
        console.print(f"  ! {object_key} skipped, {reason}")

    if plan.is_empty():
        console.print("Already up to date.")
        return

    if plan.renamed:
        console.print(
            f"\n[bold yellow]{len(plan.renamed)} renames will break queries "
            f"using the old names.[/bold yellow]"
        )

    # A plan that drops every view and creates none needs a human even under --yes:
    # it is what a mistyped --db-path, a never-synced warehouse (caught above by
    # MissingCatalogs), or a genuinely emptied app all look like.
    needs_confirmation = not yes or plan.drops_everything()
    if needs_confirmation:
        if plan.drops_everything():
            console.print(
                "[bold red]This plan drops every view and creates none.[/bold red] "
                "That always requires interactive confirmation, even with --yes."
            )
        if not _stdin_is_tty():
            # "Re-run with --yes" is only true advice when --yes was not already
            # given - a drops-everything plan refuses no matter how many times
            # --yes is passed, and telling the operator to retry it would be a lie
            # in exactly the path this guard exists to catch.
            hint = "" if yes else " Re-run with --yes if this is intended."
            console.print(
                f"[bold red]Refusing to apply without a terminal to confirm at.[/bold red]"
                f"{hint}"
            )
            raise typer.Exit(code=1)
        if not typer.confirm("Apply?"):
            raise typer.Exit(code=1)

    try:
        created = apply_label_views(pipeline, plan)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from e
    console.print(f"Rebuilt {created} views in {plan.labels_schema}")


if __name__ == "__main__":
    cli()
