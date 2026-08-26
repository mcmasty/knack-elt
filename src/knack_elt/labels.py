"""The label view layer: current Knack names over immutable physical tables.

Nothing here is app-specific. Names come from the catalogs in the warehouse, and
every identifier this module emits lands in views - never in a table, a column, or
anything a record's SCD2 history depends on.
"""

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
