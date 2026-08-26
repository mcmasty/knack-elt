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
