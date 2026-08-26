"""dlt resources and transformers for extracting a Knack application into DuckDB/MotherDuck.

Nothing in this module is app-specific: every name, object key, and credential is
passed in by the caller (see `cli.py`).
"""
import logging

import dlt
from dlt.common.normalizers.naming.snake_case import NamingConvention
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from knack_sleuth import Application

from .mapping import create_app_mappings, remap_keys

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

# dlt snake_cases whatever table name we hand it, *after* we choose it. Deduping on
# the raw Knack object name is therefore not enough.
_NAMING = NamingConvention()


def _normalized_table_name(name: str) -> str | None:
    """The table dlt will actually create for `name`, or None if it is unusable."""
    try:
        normalized = _NAMING.normalize_table_identifier(name)
    except ValueError:
        # dlt raises on a name with nothing to normalize (e.g. only whitespace).
        return None
    if normalized.startswith("_"):
        # dlt owns the underscore prefix for its own tables (_dlt_loads and friends).
        return None
    return normalized


def destination_table_name(object_name: str, object_key: str, seen: dict) -> str:
    """Pick a table name that is still unique after dlt normalizes it.

    Knack object names are not unique, and normalization collapses more of them than
    the raw strings suggest: "Order Items" and "order-items" both become
    `order_items`, and *any* name with no ASCII alphanumerics becomes `x` - so two
    unrelated non-Latin objects would share one table. That is not merely untidy:
    each run's SCD2 merge would retire the other object's rows as deleted-in-Knack,
    silently and cumulatively.

    Falls back to the Knack object key, which is unique app-wide and always ASCII.
    """
    for candidate in (object_name, f"{object_name}_{object_key}", object_key):
        normalized = _normalized_table_name(candidate)
        if normalized and normalized not in seen:
            if candidate != object_name:
                logger.warning(
                    f"Object name {object_name!r} ({object_key}) normalizes to a table "
                    f"already taken by {seen.get(_normalized_table_name(object_name))}; "
                    f"loading it as {candidate!r}"
                )
            seen[normalized] = object_key
            return candidate
    raise ValueError(f"Could not derive a unique table name for {object_key}")


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


def get_knack_table_data(table_name, object_id, client, skip_unreadable=False):
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
                        logger.warning(
                            f"Skipping a record with no id in {table_name}; "
                            f"its fields were {sorted(row)}"
                        )
                        continue
                    # Rename before anything else so the merge key is set even if a
                    # user-defined field also wants the "id" column. This mutates the
                    # row in place; each page is freshly deserialized JSON, so nothing
                    # upstream can observe it, and copying every row is not free.
                    row[RECORD_KEY] = row.pop('id')
                    row[LINEAGE_TABLE_NAME] = table_name
                    row[LINEAGE_OBJECT_ID] = object_id

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
    """Empty string -> None for numeric-ish fields, so the column does not type as text.

    Runs before the remap, so it matches raw Knack field keys. Iterating the row and
    testing membership keeps this O(row) rather than a scan of every field in the app,
    per row.
    """
    for key, value in row.items():
        if value == "" and key in numeric_fields:
            row[key] = None
    return row


def assign_default_values(row, default_values):
    """Assign default values to fields that are None."""
    for fk in row:
        if fk in default_values and (row[fk] is None or row[fk] == ""):
            row[fk] = default_values[fk]
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
    field_mappings, _object_mappings, numeric_fields, default_values = create_app_mappings(kn_app)

    # Resource names key off obj.key (globally unique in Knack); destination table
    # names come from the object name, which is NOT unique even before dlt normalizes
    # it - see destination_table_name.
    seen_tables = {}
    for obj in kn_app.objects:
        table_name = destination_table_name(obj.name, obj.key, seen_tables)

        table_resource = get_knack_table_data(table_name, obj.key, client, skip_unreadable=skip_unreadable)
        transformer_resource = get_remap_transformer(
            table_name, obj.key, field_mappings, numeric_fields, default_values
        )
        resources.append(table_resource | transformer_resource)

    logger.info(f"Built {len(resources)} resources for {kn_app.name}")
    return resources
