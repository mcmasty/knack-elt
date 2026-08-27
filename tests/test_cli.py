"""Validation tests for the CLI surface.

Every case here fails before any network call, so nothing needs credentials or a
Knack app. These cover the only thing a user actually invokes; the pipeline itself
is exercised in `test_pipeline_offline.py`.
"""
import dlt
import duckdb
import pytest
from typer.testing import CliRunner

from knack_elt import __version__
from knack_elt.cli import cli

runner = CliRunner()


def _synced_cli_warehouse(tmp_path, app_id, objects, fields, rows):
    """A warehouse at the exact pipeline/dataset `refresh-views --app-id app_id
    --destination local --db-path <tmp_path>/w.duckdb` would resolve to.

    Distinct `app_id` per test matters: the pipeline name is derived from it, and
    dlt keys its local working-dir state on pipeline name, so reusing one app_id
    across tests that actually `pipeline.run()` data bleeds schema state between
    them (the same reason `test_labels.py`'s `_synced` takes a `name=` per test).
    """
    import knack_elt.cli as cli_module

    app_identifier = cli_module.stable_app_identifier(app_id)
    db_path = tmp_path / "w.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name=f"knack_{app_identifier}_pipeline",
        dataset_name=app_identifier,
        dev_mode=False,
        destination=dlt.destinations.duckdb(str(db_path)),
    )
    for object_key, table_rows in rows.items():
        pipeline.run(
            table_rows, table_name=object_key,
            write_disposition={"disposition": "merge", "strategy": "scd2"},
            primary_key="record_id",
        )
    pipeline.run([{"object_id": o, "object_name": n} for o, n in objects],
                 table_name="_kn_object_catalog", write_disposition="replace")
    pipeline.run([{"object_id": o, "field_key": k, "field_name": n} for o, k, n in fields],
                 table_name="_kn_field_catalog", write_disposition="replace")
    return pipeline, db_path


@pytest.fixture(autouse=True)
def blank_settings(monkeypatch):
    """A developer's own .env must not decide whether these assertions hold."""
    from knack_elt.cli import settings

    monkeypatch.setattr(settings, "knack_app_id", "", raising=False)
    monkeypatch.setattr(settings, "knack_api_key", "", raising=False)
    monkeypatch.setattr(settings, "motherduck_api_key", "", raising=False)


def test_version_flag_reports_the_package_version():
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_missing_app_id_is_rejected():
    result = runner.invoke(cli, ["run-pipeline", "--api-key", "k"])
    assert result.exit_code == 1
    assert "app_id is required" in result.output


def test_missing_api_key_is_rejected():
    result = runner.invoke(cli, ["run-pipeline", "--app-id", "a"])
    assert result.exit_code == 1
    assert "API key is required" in result.output


def test_unknown_destination_is_rejected():
    result = runner.invoke(
        cli, ["run-pipeline", "--app-id", "a", "--api-key", "k", "--destination", "s3"]
    )
    assert result.exit_code == 1
    assert "unknown destination" in result.output


def test_db_path_with_motherduck_is_rejected(tmp_path):
    """--db-path names a local file; silently ignoring it would point the run at a
    different warehouse than the operator asked for."""
    result = runner.invoke(cli, [
        "run-pipeline", "--app-id", "a", "--api-key", "k",
        "--destination", "motherduck", "--db-path", str(tmp_path / "x.duckdb"),
    ])
    assert result.exit_code == 1
    assert "--db-path only applies" in result.output


def test_motherduck_without_a_token_is_rejected():
    result = runner.invoke(
        cli, ["run-pipeline", "--app-id", "a", "--api-key", "k", "--destination", "motherduck"]
    )
    assert result.exit_code == 1
    assert "motherduck_api_key" in result.output


def test_validation_happens_before_any_metadata_fetch(monkeypatch):
    """The checks above are only useful if nothing has hit the network yet."""
    def explode(*args, **kwargs):
        raise AssertionError("metadata was fetched before validation finished")

    monkeypatch.setattr("knack_elt.cli.load_app_metadata", explode)
    assert runner.invoke(cli, ["run-pipeline", "--api-key", "k"]).exit_code == 1
    assert runner.invoke(cli, ["run-pipeline", "--app-id", "a"]).exit_code == 1


def test_refresh_views_requires_an_app_id():
    result = runner.invoke(cli, ["refresh-views"])
    assert result.exit_code == 1
    assert "app_id is required" in result.output


def test_refresh_views_does_not_require_a_knack_api_key(tmp_path):
    """It reads the warehouse, never Knack. A missing key must not be the blocker -
    the missing catalogs should be."""
    result = runner.invoke(cli, [
        "refresh-views", "--app-id", "a", "--db-path", str(tmp_path / "empty.duckdb"), "--yes",
    ])
    assert "API key is required" not in result.output
    assert result.exit_code == 1


def test_refresh_views_refuses_a_non_tty_without_yes(tmp_path, monkeypatch):
    """A cron job that renames an analyst's views because nobody was there to answer
    is the failure this design exists to prevent.

    NOTE: as written this test passes for the wrong reason. `--db-path` points at a
    warehouse that was never synced, so `plan_label_views` raises `MissingCatalogs`
    and the command exits 1 before the TTY check ever runs - the plan's own
    monkeypatch of `cli_module.sys.stdin` is also a no-op, because Click's
    `CliRunner.invoke` substitutes its own stdin object during the call, not the one
    this patches. `test_refresh_views_non_tty_refusal_is_real_not_a_missing_catalogs_coincidence`
    below is the test that actually pins the refusal, against a warehouse with a
    genuine pending plan and through the `_stdin_is_tty` seam `cli.py` exposes for
    exactly this reason.
    """
    import knack_elt.cli as cli_module

    monkeypatch.setattr(cli_module.sys.stdin, "isatty", lambda: False, raising=False)
    result = runner.invoke(cli, ["refresh-views", "--app-id", "a",
                                 "--db-path", str(tmp_path / "x.duckdb")])
    assert result.exit_code == 1


def test_refresh_views_non_tty_refusal_is_real_not_a_missing_catalogs_coincidence(
    tmp_path, monkeypatch
):
    """Pins the actual non-TTY refusal against a warehouse with a real pending plan
    (a brand-new object to create), through the `_stdin_is_tty` seam - patching
    `sys.stdin` itself does not work under `CliRunner`, see the note above."""
    import knack_elt.cli as cli_module

    _pipeline, db_path = _synced_cli_warehouse(
        tmp_path, "cli-tty-1",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    monkeypatch.setattr(cli_module, "_stdin_is_tty", lambda: False)

    result = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-tty-1", "--db-path", str(db_path),
    ])
    assert result.exit_code == 1
    assert "Refusing to apply without a terminal" in result.output
    assert "+" in result.output  # the plan (a create) was printed

    con = duckdb.connect(str(db_path))
    schema = f"{cli_module.stable_app_identifier('cli-tty-1')}_labels"
    count = con.execute(
        "select count(*) from duckdb_views() where schema_name = ?", [schema]
    ).fetchone()[0]
    con.close()
    assert count == 0  # nothing was applied


def test_refresh_views_drops_everything_refuses_under_yes_without_a_tty(tmp_path):
    """The drops-everything guard fires even under --yes; in a non-interactive
    context there is nobody to answer it, so it must refuse outright rather than
    hang or silently apply. `CliRunner`'s stdin is already non-interactive, so no
    seam patch is needed here."""
    import knack_elt.cli as cli_module
    from knack_elt.labels import apply_label_views, plan_label_views

    pipeline, db_path = _synced_cli_warehouse(
        tmp_path, "cli-drops-1",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    with pipeline.sql_client() as sql_client:
        sql_client.truncate_tables("_kn_object_catalog")

    result = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-drops-1", "--db-path", str(db_path), "--yes",
    ])
    assert result.exit_code == 1
    assert "Refusing to apply without a terminal" in result.output
    assert "drops every view" in result.output
    # --yes was already given; telling the operator to retry with --yes would be a
    # lie in exactly this path, since a drops-everything plan refuses no matter how
    # many times --yes is passed.
    assert "Re-run with --yes" not in result.output

    con = duckdb.connect(str(db_path))
    schema = f"{cli_module.stable_app_identifier('cli-drops-1')}_labels"
    count = con.execute(
        "select count(*) from duckdb_views() where schema_name = ?", [schema]
    ).fetchone()[0]
    con.close()
    assert count == 2  # both views from the first apply are untouched


def test_refresh_views_drops_everything_still_prompts_under_yes(tmp_path, monkeypatch):
    """With a terminal present, --yes is not enough on its own for a drop-everything
    plan: declining leaves the previous views, accepting removes them."""
    import knack_elt.cli as cli_module
    from knack_elt.labels import apply_label_views, plan_label_views

    pipeline, db_path = _synced_cli_warehouse(
        tmp_path, "cli-drops-2",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    with pipeline.sql_client() as sql_client:
        sql_client.truncate_tables("_kn_object_catalog")

    monkeypatch.setattr(cli_module, "_stdin_is_tty", lambda: True)
    schema = f"{cli_module.stable_app_identifier('cli-drops-2')}_labels"

    decline = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-drops-2", "--db-path", str(db_path), "--yes",
    ], input="n\n")
    assert decline.exit_code == 1
    con = duckdb.connect(str(db_path))
    count = con.execute(
        "select count(*) from duckdb_views() where schema_name = ?", [schema]
    ).fetchone()[0]
    con.close()
    assert count == 2  # declined - previous views still there

    accept = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-drops-2", "--db-path", str(db_path), "--yes",
    ], input="y\n")
    assert accept.exit_code == 0
    con = duckdb.connect(str(db_path))
    count = con.execute(
        "select count(*) from duckdb_views() where schema_name = ?", [schema]
    ).fetchone()[0]
    con.close()
    assert count == 0  # accepted - both views dropped


def test_refresh_views_non_tty_with_yes_applies_a_normal_plan(tmp_path):
    """The cron happy path --yes exists for: no terminal, an ordinary (non-empty,
    non-drop-everything) plan, applies without prompting."""
    import knack_elt.cli as cli_module

    _pipeline, db_path = _synced_cli_warehouse(
        tmp_path, "cli-cron-1",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )

    result = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-cron-1", "--db-path", str(db_path), "--yes",
    ])
    assert result.exit_code == 0

    con = duckdb.connect(str(db_path))
    schema = f"{cli_module.stable_app_identifier('cli-cron-1')}_labels"
    names = sorted(r[0] for r in con.execute(
        "select view_name from duckdb_views() where schema_name = ?", [schema]
    ).fetchall())
    con.close()
    assert names == ["Classes", "Classes_history"]


def test_refresh_views_already_up_to_date_exits_zero_without_a_tty_or_yes(tmp_path):
    """An up-to-date cron run must not trip the non-TTY refusal - `is_empty()` short
    circuits before the confirmation logic runs at all, and CliRunner's stdin is
    already non-interactive so no seam patch is needed to prove it."""
    from knack_elt.labels import apply_label_views, plan_label_views

    pipeline, db_path = _synced_cli_warehouse(
        tmp_path, "cli-cron-2",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))

    result = runner.invoke(cli, [
        "refresh-views", "--app-id", "cli-cron-2", "--db-path", str(db_path),
    ])
    assert result.exit_code == 0
    assert "Already up to date." in result.output
    assert "Refusing to apply" not in result.output


def test_report_label_drift_is_silent_when_the_labels_schema_does_not_exist(tmp_path, capsys):
    """A warehouse that has never had `refresh-views` run against it is not
    "drifted" - it is a user who has not opted into the view layer. Reporting drift
    here would spam every `run-pipeline` for an object nobody asked to see."""
    import knack_elt.cli as cli_module

    pipeline, _db_path = _synced_cli_warehouse(
        tmp_path, "cli-drift-none",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    cli_module._report_label_drift(pipeline)
    assert capsys.readouterr().out == ""


def test_report_label_drift_is_silent_when_up_to_date(tmp_path, capsys):
    import knack_elt.cli as cli_module
    from knack_elt.labels import apply_label_views, plan_label_views

    pipeline, _db_path = _synced_cli_warehouse(
        tmp_path, "cli-drift-clean",
        [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    cli_module._report_label_drift(pipeline)
    assert capsys.readouterr().out == ""


def test_report_label_drift_prints_a_rename(tmp_path, capsys):
    import knack_elt.cli as cli_module
    from knack_elt.labels import apply_label_views, plan_label_views

    pipeline, _db_path = _synced_cli_warehouse(
        tmp_path, "cli-drift-rename",
        [("object_1", "Courses")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    pipeline.run([{"object_id": "object_1", "object_name": "Classes"}],
                 table_name="_kn_object_catalog", write_disposition="replace")

    cli_module._report_label_drift(pipeline)
    output = capsys.readouterr().out
    assert "Label drift" in output
    assert "Courses" in output and "Classes" in output
    assert "refresh-views" in output


def test_run_pipeline_exit_code_survives_a_broken_drift_check(tmp_path, monkeypatch, caplog):
    """The composed claim, not just the helper in isolation: a full `run-pipeline`
    invocation whose drift check blows up must still exit 0. Wires
    `load_app_metadata` and `create_rest_client` to the synthetic fixtures
    `test_pipeline_offline.py` uses, and breaks `plan_label_views` deliberately."""
    from types import SimpleNamespace

    from test_pipeline_offline import FakeClient, make_app, make_object

    import knack_elt.cli as cli_module

    app = make_app([make_object("object_1", "Classes", [("field_1", "Name", "short_text")])],
                    slug="cli-e2e-1")
    monkeypatch.setattr(
        cli_module, "load_app_metadata",
        lambda app_id, refresh=False: SimpleNamespace(application=app),
    )
    monkeypatch.setattr(
        cli_module, "create_rest_client",
        lambda app_id, api_key: FakeClient(
            {"object_1": [{"id": "1", "field_1": "Physics"}]}
        ),
    )

    def explode(pipeline):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli_module, "plan_label_views", explode)

    # _report_label_drift returns early, silently, when the `_labels` schema does
    # not exist yet - true for a first-ever run. Pre-create it so the broken
    # plan_label_views is actually reached instead of short-circuited.
    db_path = tmp_path / "w.duckdb"
    app_identifier = cli_module.stable_app_identifier("cli-e2e-1")
    con = duckdb.connect(str(db_path))
    con.execute(f'CREATE SCHEMA "{app_identifier}_labels"')
    con.close()

    with caplog.at_level("WARNING"):
        result = runner.invoke(cli, [
            "run-pipeline", "--app-id", "cli-e2e-1", "--api-key", "k",
            "--db-path", str(db_path),
        ])
    assert result.exit_code == 0, result.output
    # Not just "exited 0" - proof the broken plan_label_views was actually reached
    # and swallowed, not short-circuited by the schema-exists gate before it ran.
    assert any("Could not check label drift" in r.message for r in caplog.records)


def test_report_label_drift_never_raises_on_an_unexpected_error():
    """The load-bearing guarantee: `run-pipeline`'s exit code can never depend on
    this check. A generic failure (not `MissingCatalogs`) must be swallowed, not
    propagated."""
    import knack_elt.cli as cli_module

    class ExplodingPipeline:
        dataset_name = "ds"

        def sql_client(self):
            raise RuntimeError("boom")

    cli_module._report_label_drift(ExplodingPipeline())  # must not raise


def test_name_flag_pins_every_derived_identifier(tmp_path, monkeypatch):
    """One name moves db file, dataset, pipeline and labels schema together.
    Splitting them is how pipeline state ends up pointing at the wrong dataset."""
    from knack_elt.cli import resolve_destination

    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    resolved = resolve_destination("some-app-id", "local", None, name="avondale")
    _, app_identifier, dataset_name, pipeline_name, local_db_path, _ = resolved
    assert app_identifier == "avondale"
    assert dataset_name == "avondale"
    assert pipeline_name == "knack_avondale_pipeline"
    assert local_db_path.name == "knack_avondale_data.duckdb"


def test_omitting_name_keeps_the_derived_identifier(tmp_path, monkeypatch):
    from knack_elt.cli import resolve_destination, stable_app_identifier

    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    resolved = resolve_destination("some-app-id", "local", None, name=None)
    assert resolved[1] == stable_app_identifier("some-app-id")


@pytest.mark.parametrize("bad", [
    "Avondale",          # uppercase folds in DuckDB; require the folded form up front
    "1avondale",         # must start with a letter
    "avon dale",         # whitespace
    "avon-dale",         # hyphen breaks off in dlt normalization
    "app_x_12ab34cd",    # looks like a derived identifier; colliding with a real one
    "a" * 41,            # longer than the derived form ever is
    "",
])
def test_invalid_warehouse_names_are_rejected_not_transformed(bad):
    """Silently slugifying a name is how one app ends up with two warehouses."""
    result = runner.invoke(cli, ["run-pipeline", "--app-id", "a", "--api-key", "k",
                                 "--name", bad])
    assert result.exit_code == 1
    assert "name" in result.output.lower()


def test_warehouse_name_comes_from_settings_when_no_flag(monkeypatch):
    """KNACK_WAREHOUSE_NAME in .env makes the name sticky per project directory."""
    from knack_elt.cli import settings

    monkeypatch.setattr(settings, "warehouse_name", "Bad Name", raising=False)
    result = runner.invoke(cli, ["run-pipeline", "--app-id", "a", "--api-key", "k"])
    assert result.exit_code == 1
    assert "name" in result.output.lower()


def test_name_flag_beats_settings(monkeypatch, tmp_path):
    from knack_elt.cli import resolve_warehouse_name, settings

    monkeypatch.setattr(settings, "warehouse_name", "from_env", raising=False)
    assert resolve_warehouse_name("from_flag") == "from_flag"
    assert resolve_warehouse_name(None) == "from_env"


def test_refresh_views_accepts_the_same_name(tmp_path):
    """Both commands must address the same warehouse or a rename lands elsewhere."""
    result = runner.invoke(cli, ["refresh-views", "--app-id", "a",
                                 "--name", "avondale",
                                 "--db-path", str(tmp_path / "none.duckdb"), "--yes"])
    assert result.exit_code == 1
    assert "No label catalogs" in result.output


def test_mixed_usage_warns_when_the_derived_warehouse_also_exists(tmp_path, monkeypatch):
    """--name today and no flag tomorrow silently splits SCD2 history. Warn, never
    block - both warehouses are legitimate."""
    from knack_elt.cli import stable_app_identifier, warn_on_sibling_warehouse

    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    derived = stable_app_identifier("some-app-id")
    base = tmp_path / "knack-elt"
    base.mkdir(parents=True)
    (base / f"knack_{derived}_data.duckdb").touch()

    warning = warn_on_sibling_warehouse("some-app-id", "avondale", base)
    assert warning is not None and derived in warning

    (base / "knack_avondale_data.duckdb").touch()
    warning = warn_on_sibling_warehouse("some-app-id", None, base)
    assert warning is not None and "avondale" in warning


def test_no_warning_when_only_one_warehouse_exists(tmp_path):
    from knack_elt.cli import warn_on_sibling_warehouse

    base = tmp_path / "knack-elt"
    base.mkdir(parents=True)
    (base / "knack_avondale_data.duckdb").touch()
    assert warn_on_sibling_warehouse("some-app-id", "avondale", base) is None
