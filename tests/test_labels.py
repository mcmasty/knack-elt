"""Name generation for the label view layer. Pure functions - no warehouse."""
import duckdb
import pytest

from knack_elt.labels import (
    HISTORY_SUFFIX,
    PASSTHROUGH_COLUMNS,
    LabelNameCollision,
    MissingCatalogs,
    assert_globally_unique,
    build_view_specs,
    column_aliases,
    fold,
    object_view_names,
    read_catalogs,
    view_sql,
)


def test_fold_is_ascii_only():
    """DuckDB folds ASCII case only: ÉTÉ and été are distinct views, Classes and
    classes are not. Folding non-ASCII too is the safe direction - it over-collides,
    costing a suffix, where under-collision costs a whole view."""
    assert fold("Classes") == fold("CLASSES") == fold("classes")
    assert fold("Trail ") != fold("Trail")


def test_distinct_labels_are_left_verbatim():
    assert object_view_names([("object_1", "Classes"), ("object_2", "Invoices")]) == {
        "object_1": "Classes",
        "object_2": "Invoices",
    }


def test_both_sides_of_a_collision_are_suffixed():
    """If the first Classes kept the plain name, adding a second object with that
    label would silently move an existing view."""
    assert object_view_names([("object_3", "Classes"), ("object_7", "Classes")]) == {
        "object_3": "Classes__object_3",
        "object_7": "Classes__object_7",
    }


def test_case_variant_labels_collide():
    """The bug this rule exists for: CREATE OR REPLACE would silently destroy one."""
    assert object_view_names([("object_3", "Classes"), ("object_7", "classes")]) == {
        "object_3": "Classes__object_3",
        "object_7": "classes__object_7",
    }


def test_a_label_colliding_with_another_objects_history_view_is_suffixed():
    names = object_view_names([("object_1", "Classes"), ("object_2", "classes_HISTORY")])
    assert names["object_1"] != "Classes" or names["object_2"] != "classes_HISTORY"
    all_names = [n for v in names.values() for n in (v, v + HISTORY_SUFFIX)]
    assert len(all_names) == len({fold(n) for n in all_names})


def test_empty_label_falls_back_to_the_key():
    assert object_view_names([("object_1", "   "), ("object_2", "")]) == {
        "object_1": "object_1",
        "object_2": "object_2",
    }


def test_non_latin_labels_are_kept_verbatim():
    assert object_view_names([("object_1", "顧客")]) == {"object_1": "顧客"}


def test_field_labels_collide_case_insensitively():
    assert column_aliases([("field_1", "Name"), ("field_2", "name")]) == {
        "field_1": "Name__field_1",
        "field_2": "name__field_2",
    }


@pytest.mark.parametrize("reserved", PASSTHROUGH_COLUMNS)
def test_a_field_may_not_claim_a_passthrough_column(reserved):
    """All four are reserved in both views, so a field has the same name in each."""
    aliases = column_aliases([("field_1", reserved.upper()), ("field_2", "Fine")])
    assert fold(aliases["field_1"]) != fold(reserved)
    assert aliases["field_2"] == "Fine"


def test_empty_field_label_falls_back_to_the_field_key():
    assert column_aliases([("field_1", "")]) == {"field_1": "field_1"}


def test_a_residual_collision_is_raised_not_papered_over():
    """The rules are not trusted to be exhaustive. Anything that survives them must
    fail loudly before a single statement runs."""
    with pytest.raises(LabelNameCollision) as exc:
        assert_globally_unique([("Classes", "object_1"), ("classes", "object_2")])
    assert "object_1" in str(exc.value) and "object_2" in str(exc.value)


def test_globally_unique_names_pass():
    assert assert_globally_unique([("Classes", "object_1"), ("Invoices", "object_2")]) is None


class FakeSqlClient:
    """Just enough of dlt's SqlClientBase for the pure-SQL helpers."""

    def __init__(self, con, dataset):
        self.con, self.dataset = con, dataset

    def execute_sql(self, sql, *args):
        # dlt's sql_client uses %s placeholders; duckdb's own API uses ?.
        return self.con.execute(sql.replace("%s", "?"), args).fetchall()

    def escape_column_name(self, name):
        return '"' + name.replace('"', '""') + '"'

    def make_qualified_table_name(self, name):
        return f'"{self.dataset}".' + self.escape_column_name(name)

    def fully_qualified_dataset_name(self):
        return f'"{self.dataset}"'


def _warehouse(tmp_path, objects, fields, tables):
    """A DuckDB file shaped like a real post-sync warehouse."""
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute("create schema ds")
    con.execute("create table ds._kn_object_catalog (object_id varchar, object_name varchar)")
    con.executemany("insert into ds._kn_object_catalog values (?, ?)", objects)
    con.execute(
        "create table ds._kn_field_catalog "
        "(object_id varchar, field_key varchar, field_name varchar)"
    )
    con.executemany("insert into ds._kn_field_catalog values (?, ?, ?)", fields)
    for table, columns in tables.items():
        cols = ", ".join(f'"{c}" varchar' for c in columns)
        con.execute(f'create table ds."{table}" (record_id varchar, {cols}, '
                    f'_dlt_valid_from timestamp, _dlt_valid_to timestamp)')
    return con, FakeSqlClient(con, "ds")


def test_missing_catalogs_is_an_error_not_an_empty_plan(tmp_path):
    """An unsynced warehouse or a mistyped --db-path must not read as "zero objects" -
    under a full rebuild that would drop the whole layer and create nothing."""
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    con.execute("create schema ds")
    with pytest.raises(MissingCatalogs):
        read_catalogs(FakeSqlClient(con, "ds"))
    con.close()


def test_catalog_rows_come_back_as_label_pairs(tmp_path):
    con, client = _warehouse(
        tmp_path,
        [("object_1", "Classes")],
        [("object_1", "field_1", "Name")],
        {"object_1": ["field_1"]},
    )
    objects, fields = read_catalogs(client)
    assert objects == [("object_1", "Classes")]
    assert fields == {"object_1": [("field_1", "Name")]}
    con.close()


def test_a_field_with_no_physical_column_is_omitted_not_emitted(tmp_path):
    """dlt creates a column only once a value arrives. Emitting field_7 for a field
    that has never held one would fail the whole view."""
    con, client = _warehouse(
        tmp_path,
        [("object_1", "Classes")],
        [("object_1", "field_1", "Name"), ("object_1", "field_7", "Never Used")],
        {"object_1": ["field_1"]},
    )
    objects, fields = read_catalogs(client)
    specs, skipped = build_view_specs(client, objects, fields)
    assert [alias for _, alias in specs[0].columns] == ["Name"]
    assert specs[0].omitted_fields == ("field_7",)
    assert skipped == []
    con.close()


def test_an_object_with_no_physical_table_is_skipped(tmp_path):
    con, client = _warehouse(
        tmp_path,
        [("object_1", "Classes"), ("object_2", "Never Loaded")],
        [("object_1", "field_1", "Name")],
        {"object_1": ["field_1"]},
    )
    objects, fields = read_catalogs(client)
    specs, skipped = build_view_specs(client, objects, fields)
    assert [s.object_key for s in specs] == ["object_1"]
    assert skipped == [("object_2", "no physical table")]
    con.close()


def test_generated_views_run_and_split_live_from_history(tmp_path):
    con, client = _warehouse(
        tmp_path, [("object_1", "Classes")],
        [("object_1", "field_1", 'Course "Name"')], {"object_1": ["field_1"]},
    )
    con.execute("insert into ds.object_1 values ('1','Physics','2026-01-01',NULL)")
    con.execute("insert into ds.object_1 values ('2','Gone','2026-01-01','2026-02-01')")
    objects, fields = read_catalogs(client)
    specs, _ = build_view_specs(client, objects, fields)
    con.execute("create schema ds_labels")
    con.execute(view_sql(specs[0], "ds", "ds_labels", history=False))
    con.execute(view_sql(specs[0], "ds", "ds_labels", history=True))

    assert con.execute('select record_id from ds_labels."Classes"').fetchall() == [("1",)]
    assert con.execute(
        'select record_id, is_live_in_knack from ds_labels."Classes_history" order by record_id'
    ).fetchall() == [("1", True), ("2", False)]
    assert [d[0] for d in con.execute('select * from ds_labels."Classes"').description] == [
        "record_id", 'Course "Name"'
    ]
    con.close()


def test_lineage_and_dlt_bookkeeping_columns_stay_out_of_views(tmp_path):
    con, client = _warehouse(
        tmp_path, [("object_1", "Classes")],
        [("object_1", "field_1", "Name")], {"object_1": ["field_1"]},
    )
    con.execute('alter table ds.object_1 add column "_kn_object_id" varchar')
    objects, fields = read_catalogs(client)
    specs, _ = build_view_specs(client, objects, fields)
    con.execute("create schema ds_labels")
    con.execute(view_sql(specs[0], "ds", "ds_labels", history=True))
    columns = [d[0] for d in con.execute('select * from ds_labels."Classes_history"').description]
    assert "_kn_object_id" not in columns
    assert columns == ["record_id", "Name", "valid_from", "valid_to", "is_live_in_knack"]
    con.close()


def test_a_residual_column_collision_fails_before_any_sql_runs(tmp_path):
    """The column-granularity twin of the view-name assert.

    Two fields labelled `X` are suffixed to `X__field_1` / `X__field_2`, which is a
    name a third field can already hold verbatim. DuckDB does not error on a
    duplicate alias - it silently renames the later one to `X__field_1_1` - so
    nothing downstream would report that two labels became one column.
    """
    con, client = _warehouse(
        tmp_path,
        [("object_1", "Classes")],
        [("object_1", "field_1", "X"), ("object_1", "field_2", "X"),
         ("object_1", "field_3", "X__field_1")],
        {"object_1": ["field_1", "field_2", "field_3"]},
    )
    objects, fields = read_catalogs(client)
    with pytest.raises(LabelNameCollision) as exc:
        build_view_specs(client, objects, fields)
    assert "field_1" in str(exc.value) and "field_3" in str(exc.value)
    con.close()


def test_a_field_suffixed_off_a_passthrough_cannot_land_on_another_field(tmp_path):
    """A field labelled `record_id` becomes `record_id__field_1`, which is a name a
    second field can hold verbatim. Both are legal Knack labels."""
    con, client = _warehouse(
        tmp_path,
        [("object_1", "Classes")],
        [("object_1", "field_1", "record_id"), ("object_1", "field_2", "record_id__field_1")],
        {"object_1": ["field_1", "field_2"]},
    )
    objects, fields = read_catalogs(client)
    with pytest.raises(LabelNameCollision):
        build_view_specs(client, objects, fields)
    con.close()
