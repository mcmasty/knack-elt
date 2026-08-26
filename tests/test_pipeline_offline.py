"""Offline tests for the generic pipeline path.

These never touch the Knack API: metadata is a synthetic `Application` and record
fetching goes through `FakeClient`. They exist to prove the pipeline is app-agnostic
- every case here is a shape a fresh Knack app can legally have.
"""
import dlt
import duckdb
import pytest
import requests
from dlt.pipeline.exceptions import PipelineStepFailed
from knack_sleuth.models import KnackAppMetadata

from knack_elt.knack_dlt import (
    RecordCountShortfall,
    build_knack_resources,
    reconcile_scd2_tables,
)
from knack_elt.mapping import create_app_mappings


def make_object(key, name, fields, singular=None):
    singular = singular or name
    return {
        "key": key,
        "name": name,
        "inflections": {"singular": singular, "plural": f"{singular}s"},
        "fields": [{"key": k, "name": n, "type": t} for k, n, t in fields],
    }


def make_app(objects, slug="fresh-app"):
    return KnackAppMetadata.model_validate({"application": {
        "id": "fake_app_id",
        "name": "Fresh App",
        "slug": slug,
        "home_scene": {"key": "scene_1", "slug": "home"},
        "objects": objects,
    }}).application


class FakeClient:
    """Stands in for RESTClient. Objects listed in `unreadable` raise on first page."""

    def __init__(self, pages, unreadable=()):
        self.pages = pages
        self.unreadable = set(unreadable)

    def paginate(self, url, params=None):
        object_key = url.split("/")[2]
        if object_key in self.unreadable:
            # dlt's RESTClient is requests-based and raises requests' HTTPError from
            # its raise_for_status hook. Raising anything else here would let a
            # forbidden-detection bug pass the suite.
            response = requests.Response()
            response.status_code = 403
            response.url = f"https://api.knack.test{url}"
            raise requests.exceptions.HTTPError(
                f"403 Forbidden for {object_key}", response=response
            )
        yield self.pages.get(object_key, [])


class ScriptedClient(FakeClient):
    """Returns a different page on each successive run - for multi-load SCD2 tests."""

    def __init__(self, runs):
        self.runs, self.call = runs, 0

    def paginate(self, url, params=None):
        yield self.runs[min(self.call, len(self.runs) - 1)]

    def advance(self):
        self.call += 1


class PartialFailureClient(FakeClient):
    """Yields a page, then fails - a partial batch, which must never be swallowed."""

    def paginate(self, url, params=None):
        yield [{"id": "1", "field_1": "ok"}]
        raise RuntimeError("500 mid-stream")


class ImmediateFailureClient(FakeClient):
    """A non-permission failure before the first row must never be skipped."""

    def paginate(self, url, params=None):
        raise RuntimeError("network unavailable")
        yield  # pragma: no cover - makes this a generator


@pytest.fixture
def collision_app():
    """Field names that are all legal in Knack and all used to collide."""
    return make_app([make_object("object_1", "Invoices", [
        ("field_1", "Total ($)", "currency"),
        ("field_2", "Total (%)", "number"),
        ("field_3", "%", "number"),
        ("field_4", "商品名", "short_text"),
        ("field_5", "ID", "short_text"),
        ("field_6", "Invoice ID", "short_text"),
        ("field_7", "Table Name", "short_text"),
        ("field_8", "Object ID", "short_text"),
        ("field_9", "Score", "equation"),
    ], singular="Invoice")])


def load(app, client, db_path, **kwargs):
    pipeline = dlt.pipeline(
        pipeline_name=f"test_{app.slug.replace('-', '_')}",
        dataset_name="fresh_app",
        dev_mode=False,
        destination=dlt.destinations.duckdb(str(db_path)),
    )
    status = {}
    pipeline.run(build_knack_resources(app, client, extraction_status=status, **kwargs))
    reconcile_scd2_tables(pipeline, app, status)
    return duckdb.connect(str(db_path))


def test_fields_use_stable_knack_keys(collision_app):
    field_mappings, _, numeric_fields, _ = create_app_mappings(collision_app)
    columns = list(field_mappings["object_1"].values())

    assert columns == [f"field_{i}" for i in range(1, 10)]
    # Registered by Knack field key: cleaning runs before the remap, so a slug would
    # never match. The behavioural check (empty equation -> NULL) is in the load test.
    assert "field_9" in numeric_fields, "equation fields must be numeric-cleaned"


def test_records_survive_the_remap(collision_app, tmp_path):
    rows = [
        {"id": "r1", "field_1": "", "field_2": "10.5", "field_3": "1", "field_4": "x",
         "field_5": "a", "field_6": "b", "field_7": "mine", "field_8": "mine", "field_9": ""},
        {"id": "r2", "field_1": "99.5", "field_2": "20.5", "field_3": "2", "field_4": "y",
         "field_5": "c", "field_6": "d", "field_7": "yours", "field_8": "yours", "field_9": "4.2"},
    ]
    con = load(collision_app, FakeClient({"object_1": rows}), tmp_path / "t.duckdb")
    names = [c[0] for c in con.execute("select * from fresh_app.object_1 limit 0").description]
    by_id = {r[names.index("record_id")]: dict(zip(names, r, strict=True))
             for r in con.execute("select * from fresh_app.object_1").fetchall()}

    # Lineage columns are underscore-prefixed so a user field can never land on them.
    assert "_kn_table_name" in names and "_kn_object_id" in names
    assert by_id["r1"]["field_7"] == "mine"
    assert by_id["r1"]["field_8"] == "mine"

    # Both colliding labels keep their own stable field-key column.
    assert "field_1" in names and "field_2" in names

    # Empty strings in numeric-ish fields land as NULL, not ''.
    assert by_id["r1"]["field_1"] is None
    assert by_id["r1"]["field_9"] is None
    con.close()


def test_duplicate_object_names_do_not_crash():
    """Two objects with the same name must not collide into one dlt resource."""
    app = make_app([
        make_object("object_1", "Products", [("field_1", "SKU", "short_text")]),
        make_object("object_2", "Products", [("field_2", "SKU", "short_text")]),
    ])
    source = build_knack_resources(app, FakeClient({}))
    assert len(list(source.resources)) >= 2


def test_skip_unreadable_object_still_loads_the_rest(tmp_path):
    app = make_app([
        make_object("object_1", "A", [("field_1", "X", "short_text")]),
        make_object("object_2", "B", [("field_2", "Y", "short_text")]),
    ])
    client = FakeClient({"object_1": [{"id": "1", "field_1": "ok"}]}, unreadable=["object_2"])
    con = load(app, client, tmp_path / "t.duckdb", skip_unreadable=True)
    assert con.execute("select count(*) from fresh_app.object_1").fetchone()[0] == 1
    con.close()


def test_partial_failure_aborts_even_when_skipping(tmp_path):
    """A half-fetched object must not reach the SCD2 merge: the rows it failed to
    yield would be retired as if they had been deleted in Knack."""
    app = make_app([make_object("object_1", "A", [("field_1", "X", "short_text")])])
    with pytest.raises(PipelineStepFailed):
        load(app, PartialFailureClient({}), tmp_path / "t.duckdb", skip_unreadable=True)


def test_non_permission_failure_is_not_skipped(tmp_path):
    app = make_app([make_object("object_1", "A", [("field_1", "X", "short_text")])])
    with pytest.raises(PipelineStepFailed):
        load(app, ImmediateFailureClient({}), tmp_path / "t.duckdb", skip_unreadable=True)


def test_unreadable_object_aborts_by_default(tmp_path):
    app = make_app([make_object("object_1", "A", [("field_1", "X", "short_text")])])
    with pytest.raises(PipelineStepFailed):
        load(app, FakeClient({}, unreadable=["object_1"]), tmp_path / "t.duckdb")


def test_user_field_named_id_cannot_clobber_record_id(tmp_path):
    app = make_app([make_object("object_1", "Invoices", [
        ("field_1", "ID", "auto_increment"),
        ("field_2", "Amount", "currency"),
    ], singular="Invoice")])
    rows = [{"id": "knack_row_1", "field_1": "INV-001", "field_2": "10"}]
    con = load(app, FakeClient({"object_1": rows}), tmp_path / "t.duckdb")
    names = [c[0] for c in con.execute("select * from fresh_app.object_1 limit 0").description]
    row = dict(zip(names, con.execute("select * from fresh_app.object_1").fetchall()[0], strict=True))

    assert row["field_1"] == "INV-001"
    assert row["record_id"] == "knack_row_1", "Knack's row id belongs in record_id"
    con.close()


def test_knack_auto_record_id_field_cannot_clobber_the_merge_key(tmp_path):
    """Knack auto-adds a "Record ID" field holding a copy of the row id. Unreserved it
    would slugify onto record_id and overwrite the merge key."""
    app = make_app([make_object("object_1", "Courses", [
        ("field_1", "Record ID", "short_text"),
        ("field_2", "Title", "short_text"),
    ], singular="Course")])
    rows = [{"id": "knack_row_1", "field_1": "knack_row_1", "field_2": "Physics"}]
    con = load(app, FakeClient({"object_1": rows}), tmp_path / "t.duckdb")
    names = [c[0] for c in con.execute("select * from fresh_app.object_1 limit 0").description]
    row = dict(zip(names, con.execute("select * from fresh_app.object_1").fetchall()[0], strict=True))

    assert row["record_id"] == "knack_row_1"
    assert row["field_1"] == "knack_row_1", "the auto-field keeps its stable field key"
    con.close()


def test_app_without_the_auto_record_id_field_still_keys_correctly(tmp_path):
    """record_id comes from the payload's top-level id, not from Knack's auto-field, so
    an app predating that field keys the same way."""
    app = make_app([make_object("object_1", "Legacy", [("field_1", "Name", "short_text")])])
    con = load(app, FakeClient({"object_1": [{"id": "abc", "field_1": "x"}]}), tmp_path / "t.duckdb")
    assert con.execute("select record_id from fresh_app.object_1").fetchone()[0] == "abc"
    con.close()


def test_scd2_lifecycle_across_two_loads(tmp_path):
    """The merge key change is only really exercised by a second load: an edited record
    must retire its old version, and a record that vanishes must be retired outright."""
    app = make_app([make_object("object_1", "Courses", [("field_1", "Title", "short_text")])])
    client = ScriptedClient([
        [{"id": "r1", "field_1": "Physics"}, {"id": "r2", "field_1": "Chemistry"}],
        [{"id": "r1", "field_1": "Physics II"}],   # r1 edited, r2 deleted in Knack
    ])
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_scd2", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(app, client, extraction_status=status))
    reconcile_scd2_tables(pipeline, app, status)
    client.advance()
    status = {}
    pipeline.run(build_knack_resources(app, client, extraction_status=status))
    reconcile_scd2_tables(pipeline, app, status)

    con = duckdb.connect(str(db))
    rows = con.execute("""select record_id, field_1, _dlt_valid_to is null as live
                          from fresh_app.object_1 order by record_id, live""").fetchall()
    live = {(r[0], r[1]) for r in rows if r[2]}
    retired = {(r[0], r[1]) for r in rows if not r[2]}

    assert live == {("r1", "Physics II")}, f"live rows wrong: {rows}"
    assert ("r1", "Physics") in retired, "the edited record's old version should be retired"
    assert ("r2", "Chemistry") in retired, "a record deleted in Knack should be retired"
    con.close()


def test_default_db_dir_is_stable_across_working_directories(tmp_path, monkeypatch):
    """The default must not be CWD-relative: the same app has to keep one warehouse
    wherever the command runs, or a record's SCD2 history silently splits in two."""
    from knack_elt.cli import default_db_dir

    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    first = default_db_dir()
    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path / "elsewhere")
    assert default_db_dir() == first
    assert first.is_absolute()
    assert "tests/data" not in str(first)


def test_default_db_dir_honours_xdg_data_home(tmp_path, monkeypatch):
    from knack_elt.cli import default_db_dir

    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    assert default_db_dir() == tmp_path / "xdg" / "knack-elt"


def test_object_names_that_normalize_alike_get_separate_tables(tmp_path):
    """Mutable, colliding labels never participate in physical table identity."""
    app = make_app([
        make_object("object_1", "Order Items", [("field_1", "SKU", "short_text")]),
        make_object("object_2", "order-items", [("field_2", "SKU", "short_text")]),
    ])
    client = FakeClient({
        "object_1": [{"id": "a", "field_1": "from-object-1"}],
        "object_2": [{"id": "b", "field_2": "from-object-2"}],
    })
    con = load(app, client, tmp_path / "t.duckdb")
    tables = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_schema='fresh_app'"
    ).fetchall()]
    data_tables = [t for t in tables if not t.startswith("_dlt")]
    assert len(data_tables) == 2, f"the two objects shared a table: {data_tables}"
    con.close()


def test_non_latin_object_names_do_not_collide(tmp_path):
    """Non-Latin labels do not matter because immutable object keys name tables."""
    app = make_app([
        make_object("object_1", "顧客", [("field_1", "Name", "short_text")]),
        make_object("object_2", "注文", [("field_2", "Total", "number")]),
    ])
    client = FakeClient({
        "object_1": [{"id": "a", "field_1": "x"}],
        "object_2": [{"id": "b", "field_2": "1"}],
    })
    con = load(app, client, tmp_path / "t.duckdb")
    data_tables = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_schema='fresh_app'"
    ).fetchall() if not r[0].startswith("_dlt")]
    assert len(data_tables) == 2, f"non-Latin object names collided: {data_tables}"
    assert not any(t.startswith("_") for t in data_tables), "dlt owns the underscore prefix"
    con.close()


def test_colliding_objects_do_not_retire_each_others_rows(tmp_path):
    """The real cost of a shared table: on the next run, one object's SCD2 merge
    retires the other object's rows, falsely marking them deleted in Knack."""
    app = make_app([
        make_object("object_1", "Order Items", [("field_1", "SKU", "short_text")]),
        make_object("object_2", "order-items", [("field_2", "SKU", "short_text")]),
    ])
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_collide", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    # Fresh dicts per run: the resource renames `id` in place, so a row cannot be
    # replayed - which is fine against a live API, where every page is new JSON.
    pipeline.run(build_knack_resources(app, FakeClient({
        "object_1": [{"id": "a", "field_1": "keep-me"}],
        "object_2": [{"id": "b", "field_2": "keep-me-too"}],
    })))
    # Second run: object_2 returns nothing. It must not retire object_1's record.
    pipeline.run(build_knack_resources(app, FakeClient({
        "object_1": [{"id": "a", "field_1": "keep-me"}],
    })))

    con = duckdb.connect(str(db))
    # dlt stores the normalized name, so ask the catalog rather than guessing.
    tables = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_schema='fresh_app'"
    ).fetchall() if not r[0].startswith("_dlt")]
    live = {t: con.execute(
        f'select count(*) from fresh_app."{t}" where _dlt_valid_to is null'
    ).fetchone()[0] for t in tables}

    assert sum(live.values()) == 2, (
        f"a record was retired by an unrelated object's merge: {live}")
    con.close()


def test_record_id_label_never_claims_the_merge_key():
    """Tripwire, not behavior: under identity mapping this holds trivially. It fails
    the moment anyone reintroduces label-derived column names."""
    app = make_app([make_object("object_1", "顧客", [
        ("field_1", "Record ID", "short_text"),
        ("field_2", "Name", "short_text"),
    ], singular="顧客")])
    field_mappings, _, _, _ = create_app_mappings(app)
    slugs = list(field_mappings["object_1"].values())

    assert "record_id" not in slugs, f"a field claimed the merge key's column: {slugs}"
    assert len(slugs) == len(set(slugs)), slugs


def test_colliding_labels_keep_distinct_stable_columns():
    """Tripwire, not behavior - see the note on the test above."""
    app = make_app([make_object("object_1", "Invoices", [
        ("field_1", "Total ($)", "currency"),
        ("field_2", "Total (%)", "number"),
        ("field_13", "Total field 2", "short_text"),
    ], singular="Invoice")])
    field_mappings, _, _, _ = create_app_mappings(app)
    slugs = list(field_mappings["object_1"].values())
    assert len(slugs) == len(set(slugs)), f"fallback collided: {slugs}"


class CountingClient(FakeClient):
    """Mimics Knack's envelope: pages carry a `total_records` the paginator drops."""

    def __init__(self, pages, totals):
        super().__init__(pages)
        self.totals = totals  # object_key -> total_records reported, or list per page

    def paginate(self, url, params=None):
        object_key = url.split("/")[2]
        reported = self.totals.get(object_key)
        rows = self.pages.get(object_key, [])
        totals = reported if isinstance(reported, list) else [reported]
        for total in totals:
            page = _PageWithResponse(rows, total)
            rows = []  # subsequent pages empty; the count check is what matters
            yield page


class _PageWithResponse(list):
    def __init__(self, rows, total):
        super().__init__(rows)
        self.response = _FakeResponse(total)


class _FakeResponse:
    def __init__(self, total):
        self._total = total

    def json(self):
        if self._total is None:
            raise ValueError("no envelope")
        return {"total_records": self._total}


def _one_object_app():
    return make_app([make_object("object_1", "A", [("field_1", "X", "short_text")])])


def test_shortfall_against_total_records_aborts(tmp_path):
    """A record that slid across a page boundary must not reach the merge, which
    would retire it as deleted in Knack."""
    client = CountingClient({"object_1": [{"id": "1", "field_1": "a"}]}, {"object_1": 5})
    with pytest.raises(PipelineStepFailed) as exc:
        load(_one_object_app(), client, tmp_path / "t.duckdb")
    # dlt wraps the resource's exception; the cause is what we care about.
    causes = []
    err = exc.value
    while err is not None:
        causes.append(type(err))
        err = err.__cause__ or err.__context__
    assert RecordCountShortfall in causes, f"wrong exception chain: {causes}"


def test_shortfall_is_not_swallowed_by_skip_unreadable(tmp_path):
    """--skip-unreadable covers objects that fail before yielding; a short batch is a
    different animal and must still abort."""
    client = CountingClient({"object_1": []}, {"object_1": 5})
    with pytest.raises(PipelineStepFailed):
        load(_one_object_app(), client, tmp_path / "t.duckdb", skip_unreadable=True)


def test_matching_count_loads_normally(tmp_path):
    client = CountingClient({"object_1": [{"id": "1", "field_1": "a"}]}, {"object_1": 1})
    con = load(_one_object_app(), client, tmp_path / "t.duckdb")
    assert con.execute("select count(*) from fresh_app.object_1").fetchone()[0] == 1
    con.close()


def test_records_added_mid_run_do_not_trip_the_check(tmp_path):
    """The count rising between the first and last page is a concurrent insert, not a
    miss - the floor is what was there throughout."""
    client = CountingClient({"object_1": [{"id": "1", "field_1": "a"}]}, {"object_1": [1, 9]})
    con = load(_one_object_app(), client, tmp_path / "t.duckdb")
    assert con.execute("select count(*) from fresh_app.object_1").fetchone()[0] == 1
    con.close()


def test_missing_total_records_skips_the_check(tmp_path):
    """Reconciliation is best-effort: an envelope without the count must not break."""
    client = CountingClient({"object_1": [{"id": "1", "field_1": "a"}]}, {"object_1": None})
    con = load(_one_object_app(), client, tmp_path / "t.duckdb")
    assert con.execute("select count(*) from fresh_app.object_1").fetchone()[0] == 1
    con.close()


def test_genuinely_empty_object_is_consistent(tmp_path):
    """Zero fetched and zero reported agree, so this is not a shortfall."""
    load(_one_object_app(), CountingClient({"object_1": []}, {"object_1": 0}),
         tmp_path / "t.duckdb")


def test_confirmed_empty_object_retires_previously_live_rows(tmp_path):
    app = _one_object_app()
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_empty", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(
        app, FakeClient({"object_1": [{"id": "1", "field_1": "a"}]}),
        extraction_status=status,
    ))
    reconcile_scd2_tables(pipeline, app, status)

    status = {}
    pipeline.run(build_knack_resources(
        app, CountingClient({"object_1": []}, {"object_1": 0}),
        extraction_status=status,
    ))
    reconcile_scd2_tables(pipeline, app, status)

    con = duckdb.connect(str(db))
    assert con.execute(
        "select _dlt_valid_to is not null from fresh_app.object_1 where record_id='1'"
    ).fetchone()[0]
    con.close()


def test_object_removed_from_metadata_retires_its_live_rows(tmp_path):
    first_app = make_app([
        make_object("object_1", "Old", [("field_1", "X", "short_text")]),
        make_object("object_2", "Keep", [("field_2", "Y", "short_text")]),
    ])
    second_app = make_app([
        make_object("object_2", "Keep", [("field_2", "Y", "short_text")]),
    ])
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_removed", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(first_app, FakeClient({
        "object_1": [{"id": "gone", "field_1": "a"}],
        "object_2": [{"id": "keep", "field_2": "b"}],
    }), extraction_status=status))
    reconcile_scd2_tables(pipeline, first_app, status)

    status = {}
    pipeline.run(build_knack_resources(second_app, FakeClient({
        "object_2": [{"id": "keep", "field_2": "b"}],
    }), extraction_status=status))
    reconcile_scd2_tables(pipeline, second_app, status)

    con = duckdb.connect(str(db))
    assert con.execute(
        "select _dlt_valid_to is not null from fresh_app.object_1 where record_id='gone'"
    ).fetchone()[0]
    assert con.execute(
        "select _dlt_valid_to is null from fresh_app.object_2 where record_id='keep'"
    ).fetchone()[0]
    con.close()


def test_object_and_field_renames_keep_one_physical_identity(tmp_path):
    first_app = make_app([
        make_object("object_1", "Courses", [("field_1", "Title", "short_text")]),
    ])
    renamed_app = make_app([
        make_object("object_1", "Classes", [("field_1", "Name", "short_text")]),
    ], slug="renamed-app")
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_rename", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(first_app, FakeClient({
        "object_1": [{"id": "1", "field_1": "Physics"}],
    }), extraction_status=status))
    reconcile_scd2_tables(pipeline, first_app, status)

    status = {}
    pipeline.run(build_knack_resources(renamed_app, FakeClient({
        "object_1": [{"id": "1", "field_1": "Physics II"}],
    }), extraction_status=status))
    reconcile_scd2_tables(pipeline, renamed_app, status)

    con = duckdb.connect(str(db))
    data_tables = [row[0] for row in con.execute(
        "select table_name from information_schema.tables "
        "where table_schema='fresh_app' and table_name not like '_dlt%'"
    ).fetchall()]
    assert data_tables == ["object_1"]
    rows = con.execute(
        "select field_1, _dlt_valid_to is null from fresh_app.object_1 "
        "order by _dlt_valid_from"
    ).fetchall()
    assert rows == [("Physics", False), ("Physics II", True)]
    con.close()


def test_zero_rows_without_a_reported_total_does_not_retire(tmp_path):
    """Reconciliation fails open: an empty page Knack never counted is not proof the
    object is empty, and retiring on it would delete history on a source hiccup."""
    app = _one_object_app()
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_fail_open", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    client = ScriptedClient([
        [{"id": "1", "field_1": "here"}],
        [],  # no envelope, so no total_records - the count is unknown, not zero
    ])
    status = {}
    pipeline.run(build_knack_resources(app, client, extraction_status=status))
    reconcile_scd2_tables(pipeline, app, status)

    client.advance()
    status = {}
    pipeline.run(build_knack_resources(app, client, extraction_status=status))
    assert status["object_1"] == {
        "completed": True, "skipped": False, "yielded": 0, "totals": []
    }
    assert reconcile_scd2_tables(pipeline, app, status) == []

    con = duckdb.connect(str(db))
    assert con.execute(
        "select _dlt_valid_to is null from fresh_app.object_1 where record_id='1'"
    ).fetchone()[0]
    con.close()


def test_skipped_unreadable_object_keeps_its_live_rows(tmp_path):
    """--skip-unreadable must not look like "Knack reported zero" to reconciliation."""
    app = _one_object_app()
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_skip_keeps", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(app, FakeClient({
        "object_1": [{"id": "1", "field_1": "here"}],
    }), extraction_status=status))
    reconcile_scd2_tables(pipeline, app, status)

    status = {}
    pipeline.run(build_knack_resources(
        app, FakeClient({}, unreadable=["object_1"]),
        skip_unreadable=True, extraction_status=status,
    ))
    assert status["object_1"]["skipped"] and not status["object_1"]["completed"]
    assert reconcile_scd2_tables(pipeline, app, status) == []

    con = duckdb.connect(str(db))
    assert con.execute(
        "select _dlt_valid_to is null from fresh_app.object_1 where record_id='1'"
    ).fetchone()[0]
    con.close()


def test_rows_without_lineage_are_left_live(tmp_path):
    """A NULL _kn_object_id cannot be matched by `= %s`; retiring it would log a
    retirement that never happened."""
    app = _one_object_app()
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name="test_null_lineage", dataset_name="fresh_app",
                            dev_mode=False, destination=dlt.destinations.duckdb(str(db)))
    status = {}
    pipeline.run(build_knack_resources(app, FakeClient({
        "object_1": [{"id": "1", "field_1": "here"}],
    }), extraction_status=status))

    con = duckdb.connect(str(db))
    con.execute("update fresh_app.object_1 set _kn_object_id = NULL where record_id='1'")
    con.close()

    assert reconcile_scd2_tables(pipeline, app, status) == []
    con = duckdb.connect(str(db))
    assert con.execute(
        "select _dlt_valid_to is null from fresh_app.object_1 where record_id='1'"
    ).fetchone()[0]
    con.close()


def test_system_and_user_field_names_do_not_collide(tmp_path):
    app = make_app([make_object("object_1", "Users", [
        ("field_1", "Account Status", "short_text"),
    ])])
    con = load(app, FakeClient({"object_1": [{
        "id": "1", "account_status": "active", "field_1": "custom"
    }]}), tmp_path / "t.duckdb")
    assert con.execute(
        "select account_status, field_1 from fresh_app.object_1"
    ).fetchone() == ("active", "custom")
    con.close()


def test_legacy_slug_named_warehouse_is_flagged_not_reused(tmp_path):
    """The old file must stay a separate, untouched namespace - and be findable."""
    from knack_elt.cli import legacy_db_path, stable_app_identifier

    legacy = legacy_db_path("acme-ops", tmp_path)
    assert legacy == tmp_path / "knack_acme_ops_data.duckdb"
    current = tmp_path / f"knack_{stable_app_identifier('app/id')}_data.duckdb"
    assert legacy != current


def test_stable_app_identifier_depends_on_app_id_not_slug():
    from knack_elt.cli import schema_catalog_rows, stable_app_identifier

    assert stable_app_identifier("app/id") == stable_app_identifier("app/id")
    assert stable_app_identifier("app/id") != stable_app_identifier("app-id")
    assert stable_app_identifier("123").startswith("app_")

    objects, fields = schema_catalog_rows(_one_object_app())
    assert objects == [{"object_id": "object_1", "object_name": "A", "table_name": "object_1"}]
    assert fields[0]["column_name"] == "field_1"
    assert fields[0]["field_name"] == "X"
