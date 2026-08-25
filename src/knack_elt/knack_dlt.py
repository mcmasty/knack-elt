"""dlt resources and transformers for extracting a Knack application into DuckDB/MotherDuck.

Nothing in this module is app-specific: every name, object key, and credential is
passed in by the caller (see `cli.py`).
"""
import json
import logging
from typing import Iterable

from knack_sleuth import Application

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

from .mapping import remap_keys, create_app_mappings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Columns the pipeline stamps onto every row for lineage. Underscore-prefixed so a
# user-defined Knack field can never slugify onto one of them and clobber the value.
LINEAGE_TABLE_NAME = "_kn_table_name"
LINEAGE_OBJECT_ID = "_kn_object_id"

# Knack returns the row identifier as the top-level key "id". The pipeline renames it to
# "record_id" and merges on that, which frees the "id" column for a user-defined field of
# that name (Knack apps commonly have one). Sourcing the key from the payload rather than
# from Knack's auto-added "Record ID" field means this holds for every app, including ones
# that predate that field.
RECORD_KEY = "record_id"


def create_rest_client(app_id: str, api_key: str) -> RESTClient:
    """Build a Knack REST client for a specific application.

    Both the application id and the API key are explicit: the record endpoints
    authenticate per-app, so they must match the app whose metadata was loaded.
    """
    auth = APIKeyAuth(
        name="X-Knack-REST-API-Key",
        api_key=api_key,
        location="header"
    )
    return RESTClient(
        base_url="https://api.knack.com/v1",
        auth=auth,
        headers={"X-Knack-Application-ID": app_id},
        paginator=PageNumberPaginator(
            base_page=1,
            total_path="total_pages"
        ),
        data_selector="records"
    )


def get_knack_table_data(table_name, object_id, client, json_fields=(), skip_unreadable=False):
    @dlt.resource(name=f"table_{object_id}",
                  write_disposition={"disposition": "merge", "strategy": "scd2"},
                  primary_key=RECORD_KEY,
                  columns={RECORD_KEY: {"merge_key": False}}  # to work around a possible bug in DLT
                  )
    def table_data():
        logger.info(f"Processing table: {table_name} ({object_id})")
        kn_params = {'rows_per_page': 1000, 'format': 'raw'}
        url = f"/objects/{object_id}/records"
        yielded = 0
        try:
            for page in client.paginate(url, params=kn_params):
                logger.debug(f"Fetched {len(page)} records from {table_name}")
                for row in page:
                    if row.get('id') is None:
                        logger.warning(f"Missing Primary Key in table {table_name}: {row}")
                        continue
                    # Rename before anything else so the merge key is set even if a
                    # user-defined field also wants the "id" column.
                    row[RECORD_KEY] = row.pop('id')
                    row[LINEAGE_TABLE_NAME] = table_name
                    row[LINEAGE_OBJECT_ID] = object_id

                    row = clean_json_fields(row, json_fields)
                    yielded += 1
                    yield row
        except Exception as e:
            # Swallowing an error *after* rows were yielded would hand dlt a
            # successful partial extraction, and the SCD2 merge would retire every
            # row missing from that partial batch. Only a zero-yield failure is safe
            # to skip.
            if skip_unreadable and yielded == 0:
                logger.error(f"Skipping unreadable object {table_name} ({object_id}): {e}")
                return
            logger.error(f"Error fetching data for table {table_name}: {e}")
            raise

    return table_data()


def clean_empty_strings(row, numeric_fields):
    """Converts empty strings to None for specified numeric fields."""
    for field in numeric_fields:
        if field in row and row[field] == "":
            row[field] = None
    return row


def assign_default_values(row, default_values):
    """Assign default values to fields that are None."""
    for fk in row.keys():
        if fk in default_values and (row[fk] is None or row[fk] == ""):
            row[fk] = default_values[fk]
    return row


def clean_json_fields(row, json_fields: Iterable[str] = ()):
    """Ensure JSON-in-string fields are valid, or convert empty/invalid JSON to None.

    Knack's `format=raw` returns rich fields (file, image, connection) as dicts, not
    JSON strings, so non-string values are left untouched.
    """
    for field in json_fields:
        if field in row:
            value = row[field]
            if not isinstance(value, str):
                continue
            if value.strip() == "":
                row[field] = None  # Replace empty JSON with None
            else:
                try:
                    json.loads(value)  # Validate JSON
                except json.JSONDecodeError:
                    row[field] = None  # Replace invalid JSON with None
    return row


def get_remap_transformer(table_name, object_id, field_mappings, numeric_fields, default_values):
    @dlt.transformer(name=f"remap_{object_id}", table_name=table_name,
                     write_disposition={"disposition": "merge", "strategy": "scd2"},
                     primary_key=RECORD_KEY,
                     columns={RECORD_KEY: {"merge_key": False}}  # to work around a possible bug in DLT
                     )
    def remap_knack_field_id_to_name(row):
        # Cleaning runs before the remap, so it matches on raw Knack field keys.
        row = clean_empty_strings(row, numeric_fields)
        row = assign_default_values(row, default_values)

        field_mapping = field_mappings.get(object_id)
        if field_mapping:
            row = remap_keys(row, field_mapping)
        else:
            logger.info(f"No field mapping found for object_id {object_id}. Keeping original field names.")
        return row

    return remap_knack_field_id_to_name


@dlt.source(max_table_nesting=0)
def build_knack_resources(kn_app: Application, client: RESTClient, skip_unreadable: bool = False):
    """One resource+transformer pair per Knack object, chained with the pipe operator."""
    resources = []
    field_mappings, object_mappings, numeric_fields, default_values = create_app_mappings(kn_app)

    # Resource names key off obj.key (globally unique in Knack); destination table
    # names come from the object name, which is NOT guaranteed unique, so dedupe.
    seen_tables = {}
    for obj in kn_app.objects:
        table_name = obj.name
        if table_name in seen_tables:
            table_name = f"{obj.name}_{obj.key}"
            logger.warning(
                f"Duplicate object name {obj.name!r} ({obj.key} and {seen_tables[obj.name]}); "
                f"loading it as {table_name!r}"
            )
        seen_tables.setdefault(obj.name, obj.key)

        table_resource = get_knack_table_data(table_name, obj.key, client, skip_unreadable=skip_unreadable)
        transformer_resource = get_remap_transformer(
            table_name, obj.key, field_mappings, numeric_fields, default_values
        )
        resources.append(table_resource | transformer_resource)

    logger.info(f"Built {len(resources)} resources for {kn_app.name}")
    return resources
