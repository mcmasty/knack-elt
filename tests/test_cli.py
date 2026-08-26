"""Validation tests for the CLI surface.

Every case here fails before any network call, so nothing needs credentials or a
Knack app. These cover the only thing a user actually invokes; the pipeline itself
is exercised in `test_pipeline_offline.py`.
"""
import pytest
from typer.testing import CliRunner

from knack_elt import __version__
from knack_elt.cli import cli

runner = CliRunner()


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
