"""
The following functions are used to create a field mapping and remap keys for a JSON record based on
knack metadata for objects.


Goal is to keep this as lightweight and simple as possible.  To lean into dlt functionality where possible.

The input to these functions is the Application Metadata object from the Knack API, (https://api.knack.com/v1/applications/{app_id}))
"""
import logging
import re
from typing import Any

from knack_sleuth.models import Application

logger = logging.getLogger(__name__)


# Field types whose values are numeric (or, for date_time, non-textual) and so must
# have empty strings nulled before load — otherwise the column types as VARCHAR.
# The aggregate types (sum/min/max/average/count) are connection roll-ups.
NUMERIC_FIELD_TYPES = [
    'number', 'currency', 'link', 'date_time', 'auto_increment',
    'count', 'sum', 'min', 'max', 'average', 'equation', 'rating',
]


def slugify_field_name(field_name: str) -> str:
    """Lowercase snake_case a Knack field name. May return '' for a name with no
    ASCII alphanumerics (e.g. "%" or a fully non-Latin name) - callers must handle it."""
    return re.sub(r'[^a-z0-9]+', '_', field_name.lower()).strip('_')


def column_name_for_field(field_name, field_key, singular, used_slugs,
                         restricted_field_names, object_name=""):
    """The column a Knack field loads into: its slug, made unique and unreserved.

    Three things can go wrong, and every fallback is re-checked rather than trusted,
    because each one can land on exactly the name it was escaping:

    - Two distinct fields slugify identically ("Total ($)" and "Total (%)" both give
      "total"). remap_keys is last-write-wins, so one column would silently replace
      the other.
    - A name with no ASCII alphanumerics slugifies to "", collapsing every such field
      into one column.
    - The slug lands on a name the pipeline owns (`record_id`). Comparison is on the
      *slug*: Knack's auto-added field is named "Record ID", which lowercases to
      "record id" and only becomes "record_id" after slugification. The escape,
      prefixing the object's singular, itself slugifies straight back to "record_id"
      when the singular has no ASCII alphanumerics.

    `field_key` is unique app-wide and always ASCII, so suffixing it terminates.
    """
    new_key = slugify_field_name(field_name) or field_key

    if new_key in restricted_field_names:
        renamed = slugify_field_name(f"{singular} {new_key}")
        new_key = renamed if renamed not in restricted_field_names and renamed else f"{field_key}_{new_key}"

    if new_key in used_slugs or new_key in restricted_field_names:
        collided_with = used_slugs.get(new_key, "a name the pipeline reserves")
        while new_key in used_slugs or new_key in restricted_field_names:
            new_key = f"{new_key}_{field_key}"
        logger.warning(
            f"Field name {field_name!r} ({field_key}) on {object_name!r} slugifies onto "
            f"{collided_with}; loading it as {new_key!r} instead."
        )

    return new_key


def create_app_mappings(app_metadata: Application) -> tuple[
    dict[Any, dict[Any, Any]], dict[Any, Any], list[str | Any], dict[Any, Any]]:
    """
    Creates both field mappings and object mappings from the Knack app metadata.
    
    Uses KnackAppMetadata Pydantic model for validated, type-safe parsing.

    Returns:
    - field_mappings: A dictionary of dictionaries. The outer dictionary keys are object_ids,
      and the inner dictionary maps original field keys to new slugified field names.
    - object_mappings: A dictionary mapping table names to object_ids.
    - numeric_fields: List of field identifiers that should be treated as numeric.
    - default_values: Dictionary of default values for fields (primarily boolean fields).
    """
    
    # Columns the pipeline owns. A user-defined field slugifying onto one of these would
    # silently replace it, so it is renamed <singular>_<name> instead.
    #
    # 'id' is deliberately NOT reserved: the pipeline renames Knack's row id to
    # 'record_id' (see knack_dlt.RECORD_KEY), so a user field named "ID" keeps the plain
    # 'id' column it was named for.
    #
    # 'record_id' IS reserved, because Knack now auto-adds a short_text field named
    # "Record ID" to every object holding a copy of the row id (verified live 2026-08-25
    # across three apps; 3,562 Illinois records, all non-blank and all equal to 'id').
    # Left unreserved it would slugify onto the merge key and overwrite it. Reserving it
    # costs one redundant <singular>_record_id column per table, which is preferable to a
    # schema whose shape depends on whether any row's values happen to diverge.
    #
    # Not covered: user objects also return account_status, approval_status, utility_key,
    # profile_keys and profile_keys_raw. Renaming e.g. an "Account Status" field on a
    # non-user object would be gratuitous, so those are left alone.
    restricted_field_names = ['record_id']
    field_mappings = {}
    object_mappings = {}
    default_values = {}
    numeric_fields = []
    
    for obj in app_metadata.objects:
        object_id = obj.key
        object_name = obj.name
        
        # Get singular form safely with fallback
        singular = obj.inflections.singular if obj.inflections else object_name

        # Create object mapping
        object_mappings[object_name] = object_id

        # Create field mapping for this object
        field_mappings[object_id] = {}
        used_slugs = {}

        for field in obj.fields:
            field_key = field.key
            field_name = field.name
            
            new_key = column_name_for_field(
                field_name, field_key, singular, used_slugs, restricted_field_names, object_name
            )
            used_slugs[new_key] = field_key
            field_mappings[object_id][field_key] = new_key
            
            # Track numeric fields
            if field.type in NUMERIC_FIELD_TYPES:
                numeric_fields.append(new_key)
                numeric_fields.append(field_key)
                numeric_fields.append(field_name)

            # Handle boolean fields with defaults
            if field.type == 'boolean' and field.format and hasattr(field.format, '__dict__'):
                # Access format as Pydantic model with extra fields allowed
                format_dict = field.format.model_dump()
                if 'default' in format_dict:
                    field_default_value = format_dict['default']
                    default_values[field_key] = field_default_value
                    default_values[new_key] = field_default_value
                    default_values[field_name] = field_default_value

    return field_mappings, object_mappings, numeric_fields, default_values
    

def remap_keys(record: dict[str, Any], field_mapping: dict[str, str]) -> dict[str, Any]:
    """Remaps the keys of a single record using the provided field mapping."""
    return {field_mapping.get(key, key): value for key, value in record.items()}

