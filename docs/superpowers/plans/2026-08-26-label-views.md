# Label Views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an analyst browse the warehouse under the names the Knack builder shows, without any editable label ever becoming a physical identity.

**Architecture:** A generated, disposable layer of views in a separate `{stable_app_id}_labels` schema, built from the `_kn_object_catalog` / `_kn_field_catalog` tables already in the warehouse. Two views per object — live rows and full history. Rebuilt only by an explicit `knack-elt refresh-views` that shows a plan and asks; `run-pipeline` reports drift and changes nothing.

**Tech Stack:** Python 3.13, dlt (`sql_client()`, `begin_transaction()`), DuckDB / MotherDuck, Typer, pytest. Package manager is `uv` — every command runs through `uv run`.

**Spec:** `docs/superpowers/specs/2026-08-26-label-views-design.md` — read it before Task 1. The plan argues from it; where they disagree, the spec wins and you should stop and say so.

## Global Constraints

- **Nothing in `src/` may hardcode an app, slug, object key, or field name.** This is a generic, licensed extraction. Docs use `acme_ops` / `{stable_app_id}`, never a real client's names.
- **Physical identities never derive from labels.** This layer creates and drops **views only**, only inside `{stable_app_id}_labels`, only in `current_database()`. It must never issue `ALTER TABLE`, `DROP TABLE`, or any write to the data schema.
- **Comparison is folded, emission is verbatim.** DuckDB folds identifiers ASCII-case-insensitively *even when quoted*. Every name comparison uses `fold()`; every emitted identifier is the original text, quoted, with embedded `"` doubled.
- **Tests are offline.** No network, no credentials, no live app id. Synthetic fixtures plus real DuckDB files in `tmp_path`, following `tests/test_pipeline_offline.py`.
- **Verify before claiming.** Run `uv run pytest tests/ -q` and `uv run ruff check src/ tests/` and paste real output. A task is not done because the code looks right.
- Baseline on this branch: **44 tests passing, ruff clean.** Never finish a task below that count.

---

### Task 1: Name generation

Pure functions. No database, no dlt, no I/O — this is the part the whole design rests on, and it must be testable without a warehouse.

**Files:**
- Create: `src/knack_elt/labels.py`
- Create: `tests/test_labels.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `PASSTHROUGH_COLUMNS: tuple[str, ...] = ("record_id", "valid_from", "valid_to", "is_live_in_knack")`
  - `HISTORY_SUFFIX: str = "_history"`
  - `class LabelNameCollision(Exception)`
  - `def fold(name: str) -> str`
  - `def object_view_names(objects: list[tuple[str, str]]) -> dict[str, str]` — `[(object_key, label)]` → `{object_key: view_name}`
  - `def column_aliases(fields: list[tuple[str, str]]) -> dict[str, str]` — `[(field_key, label)]` → `{field_key: alias}`
  - `def assert_globally_unique(names: list[tuple[str, str]]) -> None` — `[(name, owner)]`; raises `LabelNameCollision`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_labels.py
"""Name generation for the label view layer. Pure functions - no warehouse."""
import pytest

from knack_elt.labels import (
    HISTORY_SUFFIX,
    PASSTHROUGH_COLUMNS,
    LabelNameCollision,
    assert_globally_unique,
    column_aliases,
    fold,
    object_view_names,
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
    all_names = [n for k, v in names.items() for n in (v, v + HISTORY_SUFFIX)]
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
    assert_globally_unique([("Classes", "object_1"), ("Invoices", "object_2")]) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_labels.py -q`
Expected: collection error — `ModuleNotFoundError: No module named 'knack_elt.labels'`

- [ ] **Step 3: Implement**

```python
# src/knack_elt/labels.py
"""The label view layer: current Knack names over immutable physical tables.

Nothing here is app-specific. Names come from the catalogs in the warehouse, and
every identifier this module emits lands in views - never in a table, a column, or
anything a record's SCD2 history depends on.
"""
import re

PASSTHROUGH_COLUMNS = ("record_id", "valid_from", "valid_to", "is_live_in_knack")
HISTORY_SUFFIX = "_history"

_ASCII_UPPER = re.compile(r"[A-Z]")


class LabelNameCollision(Exception):
    """Two things wanted the same identifier after every disambiguation rule ran."""


def fold(name: str) -> str:
    """The comparison form of an identifier.

    DuckDB folds identifiers ASCII-case-insensitively even when quoted, so "Classes"
    and "classes" are one catalog object and CREATE OR REPLACE silently replaces
    rather than erroring. Python's lower() also folds non-ASCII, which DuckDB does
    not - that over-collides, costing an unnecessary suffix, and never loses a view.
    """
    return name.lower()


def _disambiguate(entries, suffix_key):
    """Map key -> name, suffixing *every* member of a folded collision group."""
    proposed = {key: (label.strip() or key) for key, label in entries}
    counts = {}
    for name in proposed.values():
        counts[fold(name)] = counts.get(fold(name), 0) + 1
    return {
        key: f"{name}__{suffix_key(key)}" if counts[fold(name)] > 1 else name
        for key, name in proposed.items()
    }


def object_view_names(objects: list[tuple[str, str]]) -> dict[str, str]:
    """Object key -> view name, with the history form reserved alongside each base.

    A label that matches another object's `…_history` view is a collision even
    though the base names differ, so both forms enter the count.
    """
    proposed = {key: (label.strip() or key) for key, label in objects}
    counts = {}
    for name in proposed.values():
        for form in (name, name + HISTORY_SUFFIX):
            counts[fold(form)] = counts.get(fold(form), 0) + 1
    return {
        key: f"{name}__{key}" if any(
            counts[fold(form)] > 1 for form in (name, name + HISTORY_SUFFIX)
        ) else name
        for key, name in proposed.items()
    }


def column_aliases(fields: list[tuple[str, str]]) -> dict[str, str]:
    """Field key -> column alias. Passthrough columns always win a tie."""
    reserved = {fold(name) for name in PASSTHROUGH_COLUMNS}
    proposed = {key: (label.strip() or key) for key, label in fields}
    counts = {}
    for name in proposed.values():
        counts[fold(name)] = counts.get(fold(name), 0) + 1
    return {
        key: f"{name}__{key}"
        if counts[fold(name)] > 1 or fold(name) in reserved
        else name
        for key, name in proposed.items()
    }


def assert_globally_unique(names: list[tuple[str, str]]) -> None:
    """Fail before any SQL runs if two identifiers still fold together.

    The rules above are not trusted to be exhaustive - a label can legally be
    another object's key, or another object's already-suffixed name. Asserting the
    final set turns every unforeseen case into a visible error instead of a view
    that silently replaces its neighbour.
    """
    seen = {}
    for name, owner in names:
        key = fold(name)
        if key in seen:
            raise LabelNameCollision(
                f"{owner} and {seen[key]} both resolve to the identifier {name!r}. "
                f"DuckDB folds identifiers case-insensitively, so one would silently "
                f"replace the other. Rename one of them in Knack."
            )
        seen[key] = owner
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_labels.py -q && uv run ruff check src/ tests/`
Expected: all pass, ruff clean. If `test_a_label_colliding_with_another_objects_history_view_is_suffixed` fails, the history-form reservation is wrong — fix `object_view_names`, not the test.

- [ ] **Step 5: Commit**

```bash
git add src/knack_elt/labels.py tests/test_labels.py
git commit -m "Add folded, collision-safe name generation for label views"
```

---

### Task 2: Reading the warehouse and generating view SQL

**Files:**
- Modify: `src/knack_elt/labels.py`
- Modify: `tests/test_labels.py`

**Interfaces:**
- Consumes: everything from Task 1.
- **Placeholder style:** dlt's `sql_client.execute_sql` takes `%s` placeholders, not
  `?`. `reconcile_scd2_tables` in `knack_dlt.py` is the precedent — follow it.
- Produces:
  - `class MissingCatalogs(Exception)`
  - `@dataclass(frozen=True) class ViewSpec` with fields `object_key: str`, `table_name: str`, `view_name: str`, `history_name: str`, `columns: tuple[tuple[str, str], ...]` (physical column, alias), `omitted_fields: tuple[str, ...]`
  - `def read_catalogs(sql_client) -> tuple[list[tuple[str, str]], dict[str, list[tuple[str, str]]]]` — `(objects, fields_by_object)`; raises `MissingCatalogs`
  - `def physical_columns(sql_client, table_name: str) -> set[str]`
  - `def build_view_specs(sql_client, objects, fields_by_object) -> tuple[list[ViewSpec], list[tuple[str, str]]]` — `(specs, skipped)` where skipped is `[(object_key, reason)]`
  - `def view_sql(spec: ViewSpec, data_schema: str, labels_schema: str, *, history: bool) -> str`

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_labels.py
import duckdb

from knack_elt.labels import (
    MissingCatalogs,
    build_view_specs,
    physical_columns,
    read_catalogs,
    view_sql,
)


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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_labels.py -q`
Expected: `ImportError: cannot import name 'MissingCatalogs'`

- [ ] **Step 3: Implement**

```python
# append to src/knack_elt/labels.py
from dataclasses import dataclass

OBJECT_CATALOG = "_kn_object_catalog"
FIELD_CATALOG = "_kn_field_catalog"


class MissingCatalogs(Exception):
    """The warehouse has no label catalogs to build views from."""


@dataclass(frozen=True)
class ViewSpec:
    object_key: str
    table_name: str
    view_name: str
    history_name: str
    columns: tuple[tuple[str, str], ...]
    omitted_fields: tuple[str, ...]


def read_catalogs(sql_client):
    """Current labels, straight out of the warehouse - no Knack call, no API key."""
    try:
        object_rows = sql_client.execute_sql(
            f"SELECT object_id, object_name FROM "
            f"{sql_client.make_qualified_table_name(OBJECT_CATALOG)} ORDER BY object_id"
        )
        field_rows = sql_client.execute_sql(
            f"SELECT object_id, field_key, field_name FROM "
            f"{sql_client.make_qualified_table_name(FIELD_CATALOG)} "
            f"ORDER BY object_id, field_key"
        )
    except Exception as e:
        raise MissingCatalogs(
            f"No label catalogs in {sql_client.fully_qualified_dataset_name()}. "
            f"Run `knack-elt run-pipeline` against this warehouse first, or check that "
            f"--db-path points where you think it does. Refusing to treat a warehouse "
            f"with no catalogs as an app with no objects."
        ) from e

    fields = {}
    for object_id, field_key, field_name in field_rows:
        fields.setdefault(object_id, []).append((field_key, field_name))
    return [(o, n) for o, n in object_rows], fields


def physical_columns(sql_client, table_name: str) -> set[str]:
    rows = sql_client.execute_sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        sql_client.fully_qualified_dataset_name().strip('"'),
        table_name,
    )
    return {name for (name,) in rows}


def build_view_specs(sql_client, objects, fields_by_object):
    """Turn catalog rows into buildable view definitions.

    Catalog fields with no physical column are dropped from the view and recorded;
    dlt creates a column only once a value has arrived, and naming one that does not
    exist would fail the whole view rather than one field.
    """
    view_names = object_view_names(objects)
    specs, skipped = [], []
    for object_key, _label in objects:
        present = physical_columns(sql_client, object_key)
        if not present:
            skipped.append((object_key, "no physical table"))
            continue
        catalog_fields = fields_by_object.get(object_key, [])
        aliases = column_aliases(catalog_fields)
        columns = tuple(
            (field_key, aliases[field_key])
            for field_key, _ in catalog_fields
            if field_key in present
        )
        omitted = tuple(
            field_key for field_key, _ in catalog_fields if field_key not in present
        )
        base = view_names[object_key]
        specs.append(ViewSpec(
            object_key=object_key,
            table_name=object_key,
            view_name=base,
            history_name=base + HISTORY_SUFFIX,
            columns=columns,
            omitted_fields=omitted,
        ))

    assert_globally_unique(
        [(spec.view_name, spec.object_key) for spec in specs]
        + [(spec.history_name, spec.object_key) for spec in specs]
    )
    return specs, skipped


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def view_sql(spec: ViewSpec, data_schema: str, labels_schema: str, *, history: bool) -> str:
    """One CREATE OR REPLACE VIEW statement.

    OR REPLACE is redundant after the rebuild's drop pass and kept anyway, so each
    statement is correct in isolation when pasted into a console to debug.
    """
    selected = [_quote("record_id")]
    selected += [f"{_quote(column)} AS {_quote(alias)}" for column, alias in spec.columns]
    if history:
        selected += [
            f'{_quote("_dlt_valid_from")} AS {_quote("valid_from")}',
            f'{_quote("_dlt_valid_to")} AS {_quote("valid_to")}',
            f'{_quote("_dlt_valid_to")} IS NULL AS {_quote("is_live_in_knack")}',
        ]
    name = spec.history_name if history else spec.view_name
    where = "" if history else f' WHERE {_quote("_dlt_valid_to")} IS NULL'
    return (
        f"CREATE OR REPLACE VIEW {_quote(labels_schema)}.{_quote(name)} AS "
        f"SELECT {', '.join(selected)} "
        f"FROM {_quote(data_schema)}.{_quote(spec.table_name)}{where}"
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_labels.py -q && uv run ruff check src/ tests/`
Expected: all pass, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add src/knack_elt/labels.py tests/test_labels.py
git commit -m "Read the label catalogs and generate view SQL"
```

---

### Task 3: Plan and apply

**Files:**
- Modify: `src/knack_elt/labels.py`
- Modify: `tests/test_labels.py`

**Interfaces:**
- Consumes: everything from Tasks 1 and 2.
- Produces:
  - `@dataclass(frozen=True) class LabelViewPlan` with `labels_schema: str`, `specs: tuple[ViewSpec, ...]`, `created: tuple[str, ...]`, `renamed: tuple[tuple[str, str, str], ...]` (old, new, object_key), `dropped: tuple[str, ...]`, `skipped: tuple[tuple[str, str], ...]`, and methods `is_empty() -> bool`, `drops_everything() -> bool`
  - `def labels_schema_name(dataset: str) -> str` — returns `f"{dataset}_labels"`
  - `def plan_label_views(pipeline) -> LabelViewPlan`
  - `def apply_label_views(pipeline, plan: LabelViewPlan) -> int` — returns the number of views created

Rename attribution comes from `COMMENT ON VIEW`. A view with no readable comment degrades to a created/dropped pair; the apply never depends on it.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_labels.py
import dlt

from knack_elt.labels import (
    apply_label_views,
    labels_schema_name,
    plan_label_views,
)


def _synced(tmp_path, objects, fields, rows, name="lv"):
    """A dlt pipeline whose warehouse already holds catalogs and data."""
    db = tmp_path / "t.duckdb"
    pipeline = dlt.pipeline(pipeline_name=name, dataset_name="ds", dev_mode=False,
                            destination=dlt.destinations.duckdb(str(db)))
    for object_key, table_rows in rows.items():
        pipeline.run(table_rows, table_name=object_key,
                     write_disposition={"disposition": "merge", "strategy": "scd2"},
                     primary_key="record_id")
    pipeline.run([{"object_id": o, "object_name": n} for o, n in objects],
                 table_name="_kn_object_catalog", write_disposition="replace")
    pipeline.run([{"object_id": o, "field_key": k, "field_name": n} for o, k, n in fields],
                 table_name="_kn_field_catalog", write_disposition="replace")
    return pipeline, db


def test_apply_creates_both_views_and_a_second_apply_is_a_no_op(tmp_path):
    pipeline, db = _synced(
        tmp_path, [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    plan = plan_label_views(pipeline)
    assert apply_label_views(pipeline, plan) == 2
    con = duckdb.connect(str(db))
    first = sorted(r[0] for r in con.execute(
        "select view_name from duckdb_views() where schema_name='ds_labels'").fetchall())
    assert first == ["Classes", "Classes_history"]
    con.close()

    again = plan_label_views(pipeline)
    assert again.is_empty()
    apply_label_views(pipeline, again)
    con = duckdb.connect(str(db))
    assert sorted(r[0] for r in con.execute(
        "select view_name from duckdb_views() where schema_name='ds_labels'").fetchall()) == first
    con.close()


def test_a_rename_is_attributed_and_the_stale_view_goes(tmp_path):
    pipeline, db = _synced(
        tmp_path, [("object_1", "Courses")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    pipeline.run([{"object_id": "object_1", "object_name": "Classes"}],
                 table_name="_kn_object_catalog", write_disposition="replace")

    plan = plan_label_views(pipeline)
    assert ("Courses", "Classes", "object_1") in plan.renamed
    apply_label_views(pipeline, plan)
    con = duckdb.connect(str(db))
    assert sorted(r[0] for r in con.execute(
        "select view_name from duckdb_views() where schema_name='ds_labels'").fetchall()) == [
        "Classes", "Classes_history"]
    con.close()


def test_the_rebuild_never_touches_the_physical_table(tmp_path):
    """The whole point: a label rename must leave SCD2 history byte-identical."""
    pipeline, db = _synced(
        tmp_path, [("object_1", "Courses")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    con = duckdb.connect(str(db))
    before = con.execute("select * from ds.object_1").fetchall()
    con.close()

    apply_label_views(pipeline, plan_label_views(pipeline))
    pipeline.run([{"object_id": "object_1", "object_name": "Classes"}],
                 table_name="_kn_object_catalog", write_disposition="replace")
    apply_label_views(pipeline, plan_label_views(pipeline))

    con = duckdb.connect(str(db))
    assert con.execute("select * from ds.object_1").fetchall() == before
    con.close()


def test_two_objects_swapping_names_applies_correctly(tmp_path):
    """The ordering hazard a full rebuild dissolves: incremental create-then-drop
    would transiently clobber one of these."""
    pipeline, db = _synced(
        tmp_path, [("object_1", "Alpha"), ("object_2", "Beta")],
        [("object_1", "field_1", "N"), ("object_2", "field_1", "N")],
        {"object_1": [{"record_id": "1", "field_1": "a"}],
         "object_2": [{"record_id": "2", "field_1": "b"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    pipeline.run([{"object_id": "object_1", "object_name": "Beta"},
                  {"object_id": "object_2", "object_name": "Alpha"}],
                 table_name="_kn_object_catalog", write_disposition="replace")
    apply_label_views(pipeline, plan_label_views(pipeline))

    con = duckdb.connect(str(db))
    assert con.execute('select record_id from ds_labels."Beta"').fetchone() == ("1",)
    assert con.execute('select record_id from ds_labels."Alpha"').fetchone() == ("2",)
    con.close()


def test_a_hand_authored_table_blocks_the_rebuild_and_names_itself(tmp_path):
    pipeline, db = _synced(
        tmp_path, [("object_1", "Classes")], [("object_1", "field_1", "Name")],
        {"object_1": [{"record_id": "1", "field_1": "Physics"}]},
    )
    apply_label_views(pipeline, plan_label_views(pipeline))
    con = duckdb.connect(str(db))
    con.execute('create table ds_labels."Classes_manual" (x varchar)')
    con.close()
    pipeline.run([{"object_id": "object_1", "object_name": "Classes_manual"}],
                 table_name="_kn_object_catalog", write_disposition="replace")
    with pytest.raises(Exception) as exc:
        apply_label_views(pipeline, plan_label_views(pipeline))
    assert "Classes_manual" in str(exc.value)

    con = duckdb.connect(str(db))
    assert con.execute(
        "select count(*) from duckdb_views() where schema_name='ds_labels'").fetchone()[0] == 2
    con.close()


def test_labels_schema_name_is_derived_not_guessed():
    assert labels_schema_name("app_x_a1b2c3d4") == "app_x_a1b2c3d4_labels"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_labels.py -q`
Expected: `ImportError: cannot import name 'plan_label_views'`

- [ ] **Step 3: Implement**

Key points: enumerate existing views with `duckdb_views()` filtered on **both** `database_name = current_database()` and `schema_name`; wrap drop+create in `sql_client.begin_transaction()`; stamp each view with `COMMENT ON VIEW … IS '<object_key>'`.

```python
# append to src/knack_elt/labels.py
import logging

logger = logging.getLogger(__name__)


def labels_schema_name(dataset: str) -> str:
    return f"{dataset}_labels"


@dataclass(frozen=True)
class LabelViewPlan:
    labels_schema: str
    specs: tuple
    created: tuple
    renamed: tuple
    dropped: tuple
    skipped: tuple

    def is_empty(self) -> bool:
        return not (self.created or self.renamed or self.dropped)

    def drops_everything(self) -> bool:
        return bool(self.dropped) and not self.specs


def _existing_views(sql_client, labels_schema):
    """View name -> owning object key (from its comment, or None)."""
    rows = sql_client.execute_sql(
        "SELECT view_name, comment FROM duckdb_views() "
        "WHERE database_name = current_database() AND schema_name = %s",
        labels_schema,
    ) or []
    return {name: comment for name, comment in rows}


def plan_label_views(pipeline) -> LabelViewPlan:
    dataset = pipeline.dataset_name
    labels_schema = labels_schema_name(dataset)
    with pipeline.sql_client() as sql_client:
        objects, fields = read_catalogs(sql_client)
        specs, skipped = build_view_specs(sql_client, objects, fields)
        existing = _existing_views(sql_client, labels_schema)

    target = {}
    for spec in specs:
        target[spec.view_name] = spec.object_key
        target[spec.history_name] = spec.object_key

    by_object_existing = {}
    for name, owner in existing.items():
        if owner:
            by_object_existing.setdefault(owner, []).append(name)

    created, renamed, matched = [], [], set()
    for name, owner in sorted(target.items()):
        if name in existing:
            matched.add(name)
            continue
        previous = [
            old for old in by_object_existing.get(owner, [])
            if old not in target
            and old.endswith(HISTORY_SUFFIX) == name.endswith(HISTORY_SUFFIX)
        ]
        if previous:
            old = sorted(previous)[0]
            renamed.append((old, name, owner))
            matched.add(old)
        else:
            created.append(name)

    dropped = sorted(name for name in existing if name not in target and name not in matched)
    return LabelViewPlan(
        labels_schema=labels_schema,
        specs=tuple(specs),
        created=tuple(created),
        renamed=tuple(renamed),
        dropped=tuple(dropped),
        skipped=tuple(skipped),
    )


def apply_label_views(pipeline, plan: LabelViewPlan) -> int:
    """Drop the whole layer and rebuild it, in one transaction.

    Not incremental on purpose: if one object takes a name another object's view
    currently holds, every create-then-drop order transiently clobbers one of them.
    A rebuild has no intermediate state, and views are metadata.
    """
    dataset = pipeline.dataset_name
    created = 0
    with pipeline.sql_client() as sql_client:
        sql_client.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {_quote(plan.labels_schema)}")
        with sql_client.begin_transaction():
            for name in _existing_views(sql_client, plan.labels_schema):
                sql_client.execute_sql(
                    f"DROP VIEW {_quote(plan.labels_schema)}.{_quote(name)}"
                )
            for spec in plan.specs:
                for history in (False, True):
                    sql_client.execute_sql(view_sql(spec, dataset, plan.labels_schema,
                                                    history=history))
                    name = spec.history_name if history else spec.view_name
                    sql_client.execute_sql(
                        f"COMMENT ON VIEW {_quote(plan.labels_schema)}.{_quote(name)} "
                        f"IS '{spec.object_key}'"
                    )
                    created += 1
    logger.info(f"Rebuilt {created} views in {plan.labels_schema}")
    return created
```

If `test_a_hand_authored_table_blocks_the_rebuild_and_names_itself` fails because the error text lacks the name, wrap the create loop so the raised message includes the offending identifier. Do **not** make the apply skip the conflict — failing closed with the previous views intact is the required behaviour.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/ -q && uv run ruff check src/ tests/`
Expected: everything passes, including the 44 pre-existing tests.

- [ ] **Step 5: Commit**

```bash
git add src/knack_elt/labels.py tests/test_labels.py
git commit -m "Plan and apply the label view layer as a transactional rebuild"
```

---

### Task 4: The CLI

**Files:**
- Modify: `src/knack_elt/cli.py`
- Modify: `tests/test_cli.py`

**Interfaces:**
- Consumes: `plan_label_views`, `apply_label_views`, `labels_schema_name`, `MissingCatalogs`, `LabelNameCollision` from Task 3.
- Produces: a `refresh-views` Typer command, and a drift report at the end of `run_pipeline`.

Read `run_pipeline` in `src/knack_elt/cli.py` first: reuse its `--app-id` / `--api-key` / `--destination` / `--db-path` resolution and `stable_app_identifier()` verbatim so both commands address the same warehouse. `refresh-views` needs **no Knack API key** — it never contacts Knack — but `--destination motherduck` still needs `motherduck_api_key`.

- [ ] **Step 1: Write the failing tests**

```python
# append to tests/test_cli.py
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
    is the failure this design exists to prevent."""
    import knack_elt.cli as cli_module

    monkeypatch.setattr(cli_module.sys.stdin, "isatty", lambda: False, raising=False)
    result = runner.invoke(cli, ["refresh-views", "--app-id", "a",
                                 "--db-path", str(tmp_path / "x.duckdb")])
    assert result.exit_code == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_cli.py -q`
Expected: FAIL — `No such command 'refresh-views'`

- [ ] **Step 3: Implement**

Add `import sys` at the top of `cli.py`. Factor the warehouse resolution out of `run_pipeline` into a helper both commands call:

```python
def resolve_destination(app_id: str, destination: str, db_path):
    """The (dlt destination, dataset name, pipeline name, label) both commands share.

    Extracted so refresh-views cannot drift from run-pipeline: two commands deriving
    the same warehouse two ways is how a rename ends up applied to the wrong file.
    """
    app_identifier = stable_app_identifier(app_id)
    dest_db_name = f"knack_{app_identifier}_data"
    if destination == "local":
        local_db_path = (db_path or default_db_dir() / f"{dest_db_name}.duckdb").resolve()
        local_db_path.parent.mkdir(parents=True, exist_ok=True)
        return (dlt.destinations.duckdb(str(local_db_path)), app_identifier,
                f"knack_{app_identifier}_pipeline", str(local_db_path))
    return (dlt.destinations.motherduck(
        f"md:///{dest_db_name}?token={settings.motherduck_api_key}"),
        app_identifier, f"knack_{app_identifier}_pipeline",
        f"MotherDuck md:///{dest_db_name}")
```

Rewrite `run_pipeline`'s destination block to call it, then add the command:

```python
@cli.command()
def refresh_views(
    app_id: str = typer.Option(None, "--app-id",
        help="Knack application ID. Defaults to $KNACK_APP_ID."),
    destination: str = typer.Option("local", "--destination", "-d",
        help="Where the warehouse lives: 'local' or 'motherduck'."),
    db_path: Path = typer.Option(None, "--db-path",
        help="Local DuckDB file. Same default as run-pipeline."),
    yes: bool = typer.Option(False, "--yes",
        help="Apply without confirming. Required for non-interactive use."),
):
    """Rebuild the label views to match the labels from the last sync.

    Reads the catalogs already in the warehouse - no Knack API key, no network. The
    views reflect labels as of the last sync, not as of right now.
    """
    final_app_id = app_id or settings.knack_app_id
    if not final_app_id:
        console.print("[bold red]Error:[/bold red] app_id is required. Provide it via "
                      "--app-id option or set KNACK_APP_ID environment variable.")
        raise typer.Exit(code=1)
    if destination not in ("local", "motherduck"):
        console.print(f"[bold red]Error:[/bold red] unknown destination {destination!r}; "
                      f"expected 'local' or 'motherduck'.")
        raise typer.Exit(code=1)
    if destination == "motherduck" and db_path is not None:
        console.print("[bold red]Error:[/bold red] --db-path only applies to "
                      "--destination local.")
        raise typer.Exit(code=1)
    if destination == "motherduck" and not settings.motherduck_api_key:
        console.print("[bold red]Error:[/bold red] --destination motherduck requires "
                      "motherduck_api_key in the environment or .env.")
        raise typer.Exit(code=1)

    dlt_destination, dataset_name, pipeline_name, label = resolve_destination(
        final_app_id, destination, db_path)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name,
                            dev_mode=False, destination=dlt_destination)
    try:
        plan = plan_label_views(pipeline)
    except (MissingCatalogs, LabelNameCollision) as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from e

    console.print(f"Plan for {plan.labels_schema} ({label}):")
    for old, new, object_key in plan.renamed:
        console.print(f"  ~ {old!r} -> {new!r}  ({object_key})")
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
        console.print(f"\n[bold yellow]{len(plan.renamed)} renames will break queries "
                      f"using the old names.[/bold yellow]")

    needs_confirmation = not yes or plan.drops_everything()
    if needs_confirmation:
        if not sys.stdin.isatty():
            console.print("[bold red]Refusing to apply without a terminal to confirm "
                          "at.[/bold red] Re-run with --yes if this is intended.")
            raise typer.Exit(code=1)
        if plan.drops_everything():
            console.print("[bold red]This plan drops every view and creates none.[/bold red]")
        if not typer.confirm("Apply?"):
            raise typer.Exit(code=1)

    try:
        created = apply_label_views(pipeline, plan)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1) from e
    console.print(f"Rebuilt {created} views in {plan.labels_schema}")
```

Finally, at the very end of `run_pipeline` (after the `_trace` load), report drift without ever failing the command:

```python
    # Detected, not prevented. A label edit in the Knack builder must never move a
    # warehouse name on its own - a dashboard should break when someone chose it to,
    # not because a form was edited on a Tuesday.
    try:
        drift = plan_label_views(knack_dlt_pipeline)
        if not drift.is_empty():
            console.print(f"\n[bold yellow]Label drift[/bold yellow] in "
                          f"{drift.labels_schema}:")
            for old, new, object_key in drift.renamed:
                console.print(f"  {object_key}   {old!r} -> {new!r}")
            console.print("Run `knack-elt refresh-views` to update the view layer.")
    except MissingCatalogs:
        pass
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not check label drift: {e}")
```

A successful load that cannot be described is still a successful load — the drift check must never change the exit code.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/ -q && uv run ruff check src/ tests/`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/knack_elt/cli.py tests/test_cli.py
git commit -m "Add refresh-views, and report label drift from run-pipeline"
```

---

### Task 5: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `docs/ARCHITECTURE.md`
- Regenerate: `docs/ARCHITECTURE.pdf`

**Interfaces:**
- Consumes: the finished behaviour of Tasks 1–4. Read them before writing — document what the code does, not what the plan said it would.

- [ ] **Step 1: README**

Add a section after the destinations table covering: what the `_labels` schema is, the two views per object and why the split exists, that `refresh-views` is explicit and asks first, that `run-pipeline` only reports drift, and that the schema is knack-elt-managed so hand-authored views there are removed. Use `acme_ops` / `{stable_app_id}` — never a real client's names.

- [ ] **Step 2: CLAUDE.md**

Add `labels.py` to **Key Components** with its public functions, and one line under Testing for `tests/test_labels.py`. State the two rules a future editor will otherwise break: comparison is folded while emission is verbatim, and the apply is a full rebuild rather than an incremental diff.

- [ ] **Step 3: ARCHITECTURE.md**

Add the `_labels` schema to the relevant diagram section. **Render every mermaid block you touch before committing** — parse errors and unreadable layouts are invisible in markdown source:

```bash
npx -y @mermaid-js/mermaid-cli -i docs/ARCHITECTURE.md -o /tmp/arch-check.md
rm -f arch-*.svg
```

- [ ] **Step 4: Regenerate the PDF**

```bash
uv run scripts/build_architecture_pdf.py
```
Expected: `wrote docs/ARCHITECTURE.pdf`, and the page count is 12 or higher (it was 12 before this section was added). Needs node and Chrome.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/ -q && uv run ruff check src/ tests/
git add README.md CLAUDE.md docs/ARCHITECTURE.md docs/ARCHITECTURE.pdf
git commit -m "Document the label view layer"
```

---

## Done when

- `uv run pytest tests/ -q` passes with **at least 70 tests** (44 baseline + the new suite)
- `uv run ruff check src/ tests/` is clean
- `uv run knack-elt refresh-views --help` renders
- Nothing in `src/` names an app, slug, object key, or field
- A label rename leaves the physical SCD2 tables byte-identical — pinned by `test_the_rebuild_never_touches_the_physical_table`
