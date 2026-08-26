"""dlt resources and transformers for extracting a Knack application into DuckDB/MotherDuck.

Nothing in this module is app-specific: every name, object key, and credential is
passed in by the caller (see `cli.py`).
"""
import logging

import dlt
import httpx
import requests
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


class MalformedRecord(Exception):
    """A record arrived without Knack's top-level `id`.

    Every Knack record has one, so its absence means the payload is not what the
    API contract says it is. Dropping the row would quietly shrink the batch, and
    when the page envelope carries no `total_records` there is nothing left to
    notice - the same reason a short batch aborts rather than loading.
    """


class RecordCountShortfall(Exception):
    """Fewer records came back than Knack said the object holds.

    Raised instead of returning a short batch, because dlt would treat a short
    batch as a complete one and the SCD2 merge would retire every record missing
    from it as though it had been deleted in Knack.
    """


def _reported_total(page) -> int | None:
    """Knack's `total_records` for the page, or None if the envelope lacks it.

    dlt unwraps the response to the records array (data_selector), but attaches the
    raw response to the page, which is where the count lives.
    """
    try:
        return page.response.json().get("total_records")
    except Exception:
        return None

# Validate stable object keys with the same naming convention dlt applies later.
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
    """Return the stable physical table name for a Knack object.

    Object labels are editable and non-unique. Knack object keys are immutable,
    app-wide unique, and safe after dlt normalization, so labels are kept only as
    lineage metadata and never used as physical identifiers.
    """
    normalized = _normalized_table_name(object_key)
    if not normalized or normalized in seen:
        raise ValueError(f"Knack object key is not a unique usable table name: {object_key!r}")
    seen[normalized] = object_key
    return object_key


# dlt's RESTClient is requests-based, so a forbidden page raises requests' HTTPError.
# httpx is matched too because the client is injected by the caller and knack-sleuth
# uses httpx; getting this tuple wrong turns --skip-unreadable into a no-op.
_HTTP_STATUS_ERRORS = (requests.exceptions.HTTPError, httpx.HTTPStatusError)


def _is_forbidden_error(error: Exception) -> bool:
    """Whether an exception chain contains an HTTP 403 response."""
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, _HTTP_STATUS_ERRORS):
            # requests attaches no response when the failure was not a real reply.
            response = getattr(current, "response", None)
            if response is not None:
                return response.status_code == 403
        current = current.__cause__ or current.__context__
    return False


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


def get_knack_table_data(
    table_name, object_id, client, skip_unreadable=False, extraction_status=None
):
    @dlt.resource(name=f"table_{object_id}",
                  write_disposition={"disposition": "merge", "strategy": "scd2"},
                  primary_key=RECORD_KEY,
                  columns={RECORD_KEY: {"merge_key": False}}  # to work around a possible bug in DLT
                  )
    def table_data():
        status = None
        if extraction_status is not None:
            status = extraction_status.setdefault(object_id, {})
            status.update(completed=False, skipped=False, yielded=0, totals=[])
        logger.info(f"Processing table: {table_name} ({object_id})")
        kn_params = {'rows_per_page': 1000, 'format': 'raw'}
        url = f"/objects/{object_id}/records"
        yielded = 0
        totals = []
        try:
            for page in client.paginate(url, params=kn_params):
                logger.debug(f"Fetched {len(page)} records from {table_name}")
                reported = _reported_total(page)
                if reported is not None:
                    totals.append(reported)
                    if status is not None:
                        status["totals"] = list(totals)
                for row in page:
                    if row.get('id') is None:
                        raise MalformedRecord(
                            f"{table_name} ({object_id}): a record arrived without an "
                            f"id; its fields were {sorted(row)}. Loading the rest would "
                            f"hand dlt a short batch as though it were complete."
                        )
                    # Rename before anything else so the merge key is set even if a
                    # user-defined field also wants the "id" column. This mutates the
                    # row in place; each page is freshly deserialized JSON, so nothing
                    # upstream can observe it, and copying every row is not free.
                    row[RECORD_KEY] = row.pop('id')
                    row[LINEAGE_TABLE_NAME] = table_name
                    row[LINEAGE_OBJECT_ID] = object_id

                    yielded += 1
                    if status is not None:
                        status["yielded"] = yielded
                    yield row

            # Knack pages by number, not by cursor, so a record inserted or deleted
            # mid-extraction shifts the page boundaries and one can slip through the
            # gap. Compare what arrived against what Knack said it held: a record
            # present for the whole run should have been on some page. Both endpoints
            # are used because the count itself moves - the lower one is the number we
            # can be sure was there throughout.
            if totals:
                floor = min(totals[0], totals[-1])
                if yielded < floor:
                    raise RecordCountShortfall(
                        f"{table_name} ({object_id}): fetched {yielded} records but Knack "
                        f"reported at least {floor} throughout "
                        f"(first page said {totals[0]}, last said {totals[-1]}). "
                        f"Records shifted across a page boundary mid-extraction; "
                        f"loading this batch would retire the missing rows as deleted."
                    )
            logger.info(
                f"{table_name}: {yielded} records"
                + (f" (Knack reported {totals[-1]})" if totals else "")
            )
            if status is not None:
                status.update(completed=True, yielded=yielded, totals=list(totals))
        except Exception as e:
            # Swallowing an error *after* rows were yielded would hand dlt a
            # successful partial extraction, and the SCD2 merge would retire every
            # row missing from that partial batch. Only a zero-yield failure is safe
            # to skip.
            if skip_unreadable and yielded == 0 and _is_forbidden_error(e):
                if status is not None:
                    status.update(skipped=True, error=str(e))
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
def build_knack_resources(
    kn_app: Application,
    client: RESTClient,
    skip_unreadable: bool = False,
    extraction_status: dict | None = None,
):
    """One resource+transformer pair per Knack object, chained with the pipe operator."""
    resources = []
    field_mappings, _object_mappings, numeric_fields, default_values = create_app_mappings(kn_app)

    # Resource and destination names both key off obj.key. Human labels are mutable
    # metadata and cannot safely identify a physical SCD2 table.
    seen_tables = {}
    for obj in kn_app.objects:
        table_name = destination_table_name(obj.name, obj.key, seen_tables)

        table_resource = get_knack_table_data(
            table_name,
            obj.key,
            client,
            skip_unreadable=skip_unreadable,
            extraction_status=extraction_status,
        )
        transformer_resource = get_remap_transformer(
            table_name, obj.key, field_mappings, numeric_fields, default_values
        )
        resources.append(table_resource | transformer_resource)

    logger.info(f"Built {len(resources)} resources for {kn_app.name}")
    return resources


def reconcile_scd2_tables(pipeline, kn_app: Application, extraction_status: dict) -> list[str]:
    """Retire live rows for objects Knack confirmed empty or dropped from metadata.

    dlt creates no load job for an empty resource, so its normal SCD2 merge cannot
    retire the rows already present. Objects removed from metadata have no resource
    at all. Reconcile only after the extraction/load succeeds, and only treat an
    object as empty when Knack explicitly reported zero records.

    Deliberately scoped to this pipeline's own dataset. Tables from releases that
    named tables after mutable labels live in a different database and dataset (see
    the README migration note); they are unreachable from here and are left alone
    rather than guessed at.
    """
    current_tables = {obj.key: destination_table_name(obj.name, obj.key, {}) for obj in kn_app.objects}
    retired = []
    schema = pipeline.default_schema

    with pipeline.sql_client() as client:
        object_column = client.escape_column_name(LINEAGE_OBJECT_ID)
        valid_to_column = client.escape_column_name("_dlt_valid_to")

        for table in schema.data_tables(seen_data_only=True):
            columns = table["columns"]
            if LINEAGE_OBJECT_ID not in columns or "_dlt_valid_to" not in columns:
                continue

            table_name = table["name"]
            qualified_table = client.make_qualified_table_name(table_name)
            object_rows = client.execute_sql(
                f"SELECT DISTINCT {object_column} FROM {qualified_table} "
                f"WHERE {valid_to_column} IS NULL"
            ) or []

            for (object_id,) in object_rows:
                if object_id is None:
                    # `= NULL` matches nothing in SQL, so retiring these would log an
                    # action that never happened. Unattributable rows are left live.
                    logger.warning(
                        f"Live rows in {table_name} carry no {LINEAGE_OBJECT_ID}; "
                        f"leaving them open because they cannot be attributed."
                    )
                    continue

                status = extraction_status.get(object_id, {})
                confirmed_empty = (
                    status.get("completed")
                    and status.get("yielded") == 0
                    and status.get("totals")
                    and all(total == 0 for total in status["totals"])
                )
                removed_object = object_id not in current_tables
                if not (confirmed_empty or removed_object):
                    continue

                client.execute_sql(
                    f"UPDATE {qualified_table} SET {valid_to_column} = CURRENT_TIMESTAMP "
                    f"WHERE {valid_to_column} IS NULL AND {object_column} = %s",
                    object_id,
                )
                reason = "confirmed empty" if confirmed_empty else "removed from metadata"
                message = f"Retired live rows for {object_id} in {table_name}: {reason}"
                logger.warning(message)
                retired.append(message)

    return retired
