"""
The following functions are used to create a field mapping and remap keys for a JSON record based on
knack metadata for objects.


Physical column names use Knack's immutable field keys. Human-readable labels are
metadata, not identifiers: builders may rename a field at any time, and changing a
label must not move current data into a new warehouse column.

The input to these functions is the Application Metadata object from the Knack API, (https://api.knack.com/v1/applications/{app_id}))
"""
from typing import Any

from knack_sleuth.models import Application

# Field types whose values are numeric (or, for date_time, non-textual) and so must
# have empty strings nulled before load — otherwise the column types as VARCHAR.
# The aggregate types (sum/min/max/average/count) are connection roll-ups.
NUMERIC_FIELD_TYPES = [
    'number', 'currency', 'link', 'date_time', 'auto_increment',
    'count', 'sum', 'min', 'max', 'average', 'equation', 'rating',
]


def create_app_mappings(app_metadata: Application) -> tuple[
    dict[Any, dict[Any, Any]], dict[Any, Any], set[str], dict[Any, Any]]:
    """
    Creates both field mappings and object mappings from the Knack app metadata.
    
    Uses KnackAppMetadata Pydantic model for validated, type-safe parsing.

    Returns:
    - field_mappings: A dictionary of dictionaries. The outer dictionary keys are object_ids,
      and the inner dictionary maps raw field keys to stable physical column names. Both are
      currently the immutable Knack field key.
    - object_mappings: A dictionary mapping stable table names to object_ids.
    - numeric_fields: Set of Knack field keys whose values are numeric-ish.
    - default_values: Knack field key -> declared default (primarily boolean fields).
    """
    
    field_mappings = {}
    object_mappings = {}
    # Keyed by Knack field key only. Both are consumed before the remap, so entries
    # under the slug or the raw field name could never match a row and were dead
    # weight - three per field, scanned per row.
    default_values = {}
    numeric_fields = set()
    
    for obj in app_metadata.objects:
        object_id = obj.key
        object_mappings[object_id] = object_id

        # Create field mapping for this object
        field_mappings[object_id] = {}
        for field in obj.fields:
            field_key = field.key
            # Field labels are editable. The key is immutable, globally unique in the
            # app, cannot collide with top-level system keys such as account_status,
            # and therefore is the only safe physical column identity.
            field_mappings[object_id][field_key] = field_key
            
            # Track numeric fields
            if field.type in NUMERIC_FIELD_TYPES:
                numeric_fields.add(field_key)

            # Handle boolean fields with defaults
            if field.type == 'boolean' and field.format and hasattr(field.format, '__dict__'):
                # Access format as Pydantic model with extra fields allowed
                format_dict = field.format.model_dump()
                if 'default' in format_dict:
                    default_values[field_key] = format_dict['default']

    return field_mappings, object_mappings, numeric_fields, default_values
    

def remap_keys(record: dict[str, Any], field_mapping: dict[str, str]) -> dict[str, Any]:
    """Remaps the keys of a single record using the provided field mapping."""
    return {field_mapping.get(key, key): value for key, value in record.items()}
