"""
The following functions are used to create a field mapping and remap keys for a JSON record based on
knack metadata for objects.


Goal is to keep this as lightweight and simple as possible.  To lean into dlt functionality where possible.

The input to these functions is the Application Metadata object from the Knack API, (https://api.knack.com/v1/applications/{app_id}))
"""
import re
from typing import Dict, Any

from knack_sleuth.models import KnackAppMetadata, Application


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
    
    restricted_field_names = ['id']  # if the Knack User defined a field with the name 'id', it will be overwritten
    #  (cont.) with object_name_id.  Knack row id is always 'id', thus user defined stamps on it.
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

        for field in obj.fields:
            field_key = field.key
            field_name = field.name
            
            # Handle restricted field names
            if field_name.lower().strip() in restricted_field_names:
                field_name = f"{singular}_{field_name.lower()}"
            
            # Slugify field name
            new_key = re.sub(r'[^a-z0-9]+', '_', field_name.lower()).strip('_')
            field_mappings[object_id][field_key] = new_key
            
            # Track numeric fields
            if field.type in ['number', 'currency', 'link', 'date_time', 'auto_increment', 'count']:
                numeric_fields.append(new_key)
                numeric_fields.append(field_key)
                numeric_fields.append(field_name)

            # Handle boolean fields with defaults
            if field.type == 'boolean':
                if field.format and hasattr(field.format, '__dict__'):
                    # Access format as Pydantic model with extra fields allowed
                    format_dict = field.format.model_dump()
                    if 'default' in format_dict:
                        field_default_value = format_dict['default']
                        default_values[field_key] = field_default_value
                        default_values[new_key] = field_default_value
                        default_values[field_name] = field_default_value

    return field_mappings, object_mappings, numeric_fields, default_values
    

def remap_keys(record: Dict[str, Any], field_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Remaps the keys of a single record using the provided field mapping."""
    return {field_mapping.get(key, key): value for key, value in record.items()}

