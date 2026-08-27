"""The label view layer: current Knack names over immutable physical tables.

Nothing here is app-specific. Names come from the catalogs in the warehouse, and
every identifier this module emits lands in views - never in a table, a column, or
anything a record's SCD2 history depends on.
"""
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

PASSTHROUGH_COLUMNS = ("record_id", "valid_from", "valid_to", "is_live_in_knack")
HISTORY_SUFFIX = "_history"


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
        # The same assert the view names get, at column granularity. A duplicate
        # column alias is worse than a duplicate view name: DuckDB does not error,
        # it silently renames the later one, so two labels become one column with
        # nothing anywhere reporting it. Passthroughs are included even though the
        # alias rules already reserve them - the assert exists precisely because
        # those rules are not trusted to be exhaustive.
        assert_globally_unique(
            [(alias, field_key) for field_key, alias in columns]
            + [(name, "a reserved passthrough column") for name in PASSTHROUGH_COLUMNS]
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


def _literal(value: str) -> str:
    """A single-quoted SQL string. Object keys reach COMMENT ON from warehouse data,
    and warehouse data is never trusted to be free of quotes."""
    return "'" + value.replace("'", "''") + "'"


def _select_pairs(spec: ViewSpec, history: bool) -> list[tuple[str, str]]:
    """(expression, output column name) for every column a view emits, in order.

    `view_sql` and `expected_columns` both read this, so the SELECT list and the
    drift comparison can never disagree about what a view is supposed to contain.
    Two hand-maintained copies is how "already up to date" starts lying.
    """
    pairs = [(_quote("record_id"), "record_id")]
    pairs += [(_quote(column), alias) for column, alias in spec.columns]
    if history:
        pairs += [
            (_quote("_dlt_valid_from"), "valid_from"),
            (_quote("_dlt_valid_to"), "valid_to"),
            (f'{_quote("_dlt_valid_to")} IS NULL', "is_live_in_knack"),
        ]
    return pairs


def expected_columns(spec: ViewSpec, *, history: bool) -> tuple[str, ...]:
    """The column list this spec's view would have, in order."""
    return tuple(alias for _, alias in _select_pairs(spec, history))


def view_sql(spec: ViewSpec, data_schema: str, labels_schema: str, *, history: bool) -> str:
    """One CREATE OR REPLACE VIEW statement.

    OR REPLACE is redundant after the rebuild's drop pass and kept anyway, so each
    statement is correct in isolation when pasted into a console to debug.
    """
    selected = [
        f"{expression} AS {_quote(alias)}"
        for expression, alias in _select_pairs(spec, history)
    ]
    name = spec.history_name if history else spec.view_name
    where = "" if history else f' WHERE {_quote("_dlt_valid_to")} IS NULL'
    return (
        f"CREATE OR REPLACE VIEW {_quote(labels_schema)}.{_quote(name)} AS "
        f"SELECT {', '.join(selected)} "
        f"FROM {_quote(data_schema)}.{_quote(spec.table_name)}{where}"
    )


def labels_schema_name(dataset: str) -> str:
    return f"{dataset}_labels"


@dataclass(frozen=True)
class LabelViewPlan:
    labels_schema: str
    specs: tuple
    created: tuple
    renamed: tuple
    changed: tuple
    dropped: tuple
    skipped: tuple

    def is_empty(self) -> bool:
        """`changed` counts. A field rename moves no view name at all, so a plan that
        ignored it would report "already up to date" over stale column aliases."""
        return not (self.created or self.renamed or self.changed or self.dropped)

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


def view_columns(sql_client, labels_schema: str) -> dict[str, tuple[str, ...]]:
    """View name -> its column names, in order.

    This, not the stored SQL, is how drift is detected. DuckDB keeps a rewritten form
    of a view's definition - `CREATE OR REPLACE` dropped, identifiers unquoted where
    they can be, the WHERE clause parenthesized, a semicolon appended - so generated
    SQL never equals `duckdb_views().sql` and comparing them reports drift that no
    rebuild can ever clear. Column names come back verbatim, and a renamed field is
    exactly a change to this list.

    Restricted to views: a hand-authored table in the schema has columns too, and it
    is not something this layer has a target column list for.
    """
    rows = sql_client.execute_sql(
        "SELECT table_name, column_name FROM duckdb_columns() "
        "WHERE database_name = current_database() AND schema_name = %s "
        "AND table_name IN (SELECT view_name FROM duckdb_views() "
        "WHERE database_name = current_database() AND schema_name = %s) "
        "ORDER BY table_name, column_index",
        labels_schema,
        labels_schema,
    ) or []
    columns = {}
    for view_name, column_name in rows:
        columns.setdefault(view_name, []).append(column_name)
    return {name: tuple(names) for name, names in columns.items()}


def _column_change_reason(have: tuple, want: tuple) -> str:
    added = [name for name in want if name not in have]
    removed = [name for name in have if name not in want]
    parts = []
    if added:
        parts.append("+" + ", ".join(repr(name) for name in added))
    if removed:
        parts.append("-" + ", ".join(repr(name) for name in removed))
    return "columns " + ("; ".join(parts) if parts else "reordered")


def plan_label_views(pipeline) -> LabelViewPlan:
    """The target view set, diffed against what the warehouse currently holds.

    Inert: computing a plan writes nothing. Drift *is* a non-empty plan, which is what
    keeps `run-pipeline`'s report and `refresh-views` from disagreeing about what
    counts as out of date.
    """
    dataset = pipeline.dataset_name
    labels_schema = labels_schema_name(dataset)
    with pipeline.sql_client() as sql_client:
        objects, fields = read_catalogs(sql_client)
        specs, skipped = build_view_specs(sql_client, objects, fields)
        existing = _existing_views(sql_client, labels_schema)
        existing_columns = view_columns(sql_client, labels_schema)

    target, targeted_spec = {}, {}
    for spec in specs:
        for name, history in ((spec.view_name, False), (spec.history_name, True)):
            target[name] = spec.object_key
            targeted_spec[name] = (spec, history)

    by_object_existing = {}
    for name, owner in existing.items():
        if owner:
            by_object_existing.setdefault(owner, []).append(name)

    created, renamed, changed, matched = [], [], [], set()
    for name, owner in sorted(target.items()):
        if name in existing:
            matched.add(name)
            spec, history = targeted_spec[name]
            previous_owner = existing[name]
            if previous_owner and previous_owner != owner:
                # Two objects swapped labels. Every view name still exists and the
                # column lists can be identical, so only the stamped owner shows
                # that each view now points at the other object's table.
                changed.append(
                    (name, f"now built from {owner}, was {previous_owner}")
                )
            else:
                want = expected_columns(spec, history=history)
                have = existing_columns.get(name)
                if have is not None and have != want:
                    # Verbatim, order-sensitive: labels are compared folded so two of
                    # them cannot collide, but `Name` -> `NAME` is a real edit a
                    # builder made and an analyst will see.
                    changed.append((name, _column_change_reason(have, want)))
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
        changed=tuple(changed),
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
                        f"IS {_literal(spec.object_key)}"
                    )
                    created += 1
    logger.info(f"Rebuilt {created} views in {plan.labels_schema}")
    return created
