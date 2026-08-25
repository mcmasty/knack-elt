from knack_elt.config import settings
from icecream import ic

import json
import logging

from .mapping import remap_keys, create_app_mappings

from knack_sleuth import Application

import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
2


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_rest_client():
    auth = APIKeyAuth(
        name="X-Knack-REST-API-Key",
        api_key=settings.knack_api_key,
        location="header"
    )
    client = RESTClient(
        base_url="https://api.knack.com/v1",
        auth=auth,
        headers={"X-Knack-Application-ID": settings.knack_app_id},
        paginator=PageNumberPaginator(
            base_page=1,
            total_path="total_pages"
        ),
        data_selector="records"
    )
    return client


def get_write_disposition(table_name: str) -> dict:

    return {}


def get_app_mappings():
    ic("get_app_mappings")
    app_id = settings.knack_app_id
    knack_api_key = settings.knack_api_key
    try:
        response = requests.get(
            f'https://api.knack.com/v1/applications/{app_id}',
            headers={
                'X-Knack-Application-ID': app_id,
                'X-Knack-REST-API-Key': knack_api_key
            }
        )
        response.raise_for_status()
        app_metadata = response.json()
        field_mappings, object_mappings, numeric_fields, default_values = create_app_mappings(app_metadata)
        return field_mappings, object_mappings, numeric_fields, default_values
    except Exception as exc:
        logger.error(f"Failed to get app mappings: {exc}")
        raise


def get_knack_table_data(table_name, object_id, client):
    @dlt.resource(name=f"table_{table_name}",
                  write_disposition={"disposition": "merge", "strategy": "scd2"},
                  primary_key="id",
                  columns={"id": {"merge_key": False}}  # to work around a possible bug in DLT
                  )
    def table_data():
        json_fields = ["document_link"]  # Replace with your JSON fields

        ic("get_knack_table_data")
        ic(table_name)
        logger.info(f"Processing table: {table_name}")
        kn_params = {'rows_per_page': 1000, 'format': 'raw'}
        url = f"/objects/{object_id}/records"
        try:
            for page in client.paginate(url, params=kn_params):
                logger.debug(f"Fetched {len(page)} records from {table_name}")
                for row in page:
                    row['table_name'] = table_name
                    row['object_id'] = object_id
                    if 'id' not in row or row['id'] is None:
                        logger.warning(f"Missing Primary Key in table {table_name}: {row}")
                        continue

                    row = clean_json_fields(row, json_fields)
                    yield row
        except Exception as e:
            logger.error(f"Error fetching data for table {table_name}: {e}")
            raise

    return table_data()


def clean_empty_strings(row, numeric_fields):
    """Converts empty strings to None for specified numeric fields."""
    for field in numeric_fields:
        if field in row and row[field] == "":
            # print(f"Converting empty string to None for field {field}")
            row[field] = None
    return row


def assign_default_values(row, default_values):
    """Assign default values to fields that are None."""
    for fk in row.keys():
        if fk in default_values and (row[fk] is None or row[fk] == ""):
            row[fk] = default_values[fk]
    return row


def clean_json_fields(row, json_fields):
    """Ensure JSON fields are valid or convert empty/invalid JSON to None."""
    for field in json_fields:
        if field in row:
            value = row[field]
            if not value or value.strip() == "":
                row[field] = None  # Replace empty JSON with None
            else:
                try:
                    # Validate JSON
                    json.loads(value)
                except json.JSONDecodeError:
                    row[field] = None  # Replace invalid JSON with None
    return row


def get_remap_transformer(table_name, field_mappings, numeric_fields, default_values):
    @dlt.transformer(name=f"remap_{table_name}", table_name=table_name,
                     write_disposition={"disposition": "merge", "strategy": "scd2"},
                     primary_key="id",
                     columns={"id": {"merge_key": False}}  # to work around a possible bug in DLT
                     )
    def remap_knack_field_id_to_name(row):
        # First clean any empty strings in numeric fields
        row = clean_empty_strings(row, numeric_fields)
        row = assign_default_values(row, default_values)

        kn_object_id = row.get('object_id')
        field_mapping = field_mappings.get(kn_object_id)
        if field_mapping:
            row = remap_keys(row, field_mapping)
            # print(row)
            # 1/0

        else:
            logger.info(f"No field mapping found for object_id {kn_object_id}. Keeping original field names.")
        # print(row)
        # exit(1)
        return row

    return remap_knack_field_id_to_name


@dlt.source(max_table_nesting=0)
def knack_ave_source():
    client = create_rest_client()
    field_mappings, object_mappings, numeric_fields, default_values = get_app_mappings()
    resources = []
    obj_list = list(object_mappings.items())

    # obj_list = [
    #     # ("Events", "object_34"),
    #     ("Accounts", "object_2"),
    #     # ("Clients", "object_14"),
    # ]
    # obj_list = [("Invoices", "object_90")]
    # obj_list = [("Vocalist / Song Details", "object_87")]
    ic(len(obj_list))
    for table_name, object_id in obj_list:
        ic(table_name, object_id)
        # Get the data resource
        table_resource = get_knack_table_data(table_name, object_id, client)
        # ic(table_resource)
        # Get the transformer resource
        transformer_resource = get_remap_transformer(table_name, field_mappings, numeric_fields, default_values)
        # Chain the data resource and transformer
        transformed_resource = table_resource | transformer_resource
        # Append to resources
        resources.append(transformed_resource)

    ic(len(resources))
    return resources

@dlt.source(max_table_nesting=0)
def build_knack_resources(kn_app: Application):
    resources = []
    client = create_rest_client()
    field_mappings, object_mappings, numeric_fields, default_values = create_app_mappings(kn_app)
    
    obj_list = list(object_mappings.items())
    for obj in kn_app.objects:

        table_resource = get_knack_table_data(obj.name, obj.key, client)
        transformer_resource = get_remap_transformer(obj.name, field_mappings, numeric_fields, default_values)
    
        transformed_resource = table_resource | transformer_resource
        resources.append(transformed_resource)
    ic(len(resources))
    return resources
    

if __name__ == "__main__":
    logger.info("Running knack_ave_pipeline")
    # https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck
    # Set the number of load workers
    dlt.config["load.workers"] = 3
    dlt.config["truncate_staging_dataset"] = True

    dest_db_name = 'knack_avondale_data'
    try:
        knack_ave_pipeline = dlt.pipeline(
            pipeline_name="knack_ave_pipeline",
            dataset_name="avondale",
            dev_mode=False,
            # destination=dlt.destinations.duckdb(f"tests/data/{dest_db_name}.duckdb"),

            destination=dlt.destinations.motherduck(
                f"md:///{dest_db_name}?token={settings.motherduck_api_key}"
            ),
            export_schema_path="pipelines/schemas/export",
            # import_schema_path="pipelines/schemas/import",
        )
        load_info = knack_ave_pipeline.run(knack_ave_source())
        # Targeted purge & reload
        # load_info = knack_ave_pipeline.run(knack_ave_source().with_resources("remap_Vocalist / Song Details"),
        #                                    refresh="drop_resources")

        # logger.info(f"Pipeline run completed: {load_info}")
        ic(load_info.started_at, load_info.finished_at)
        ic((load_info.finished_at - load_info.started_at).in_words())
        logger.debug(knack_ave_pipeline.last_trace)
        # print human friendly extract information
        print(knack_ave_pipeline.last_trace.last_extract_info)
        # print human friendly normalization information
        print(knack_ave_pipeline.last_trace.last_normalize_info)
        # access row counts dictionary of normalize info
        print(knack_ave_pipeline.last_trace.last_normalize_info.row_counts)
        # print human friendly load information
        print(knack_ave_pipeline.last_trace.last_load_info)
        # Save load_info to destination
        knack_ave_pipeline.run([load_info], table_name="_load_info")

        # Save trace to destination
        knack_ave_pipeline.run([knack_ave_pipeline.last_trace], table_name="_trace")

    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        import sys

        sys.exit(1)
