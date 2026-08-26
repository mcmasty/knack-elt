"""The label view layer: current Knack names over immutable physical tables.

Nothing here is app-specific. Names come from the catalogs in the warehouse, and
every identifier this module emits lands in views - never in a table, a column, or
anything a record's SCD2 history depends on.
"""
from dataclasses import dataclass

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
