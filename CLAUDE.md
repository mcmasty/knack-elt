# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KnackELT is a Python ELT (Extract, Load, Transform) pipeline that extracts data from Knack databases and loads it into MotherDuck (DuckDB cloud) using the dlt (data load tool) framework.

## Development Setup

This project uses `uv` as the package manager.

### Installation
```bash
uv sync
```

### Running the Pipeline
```bash
uv run python src/knack_elt/knack_dlt.py
```

## Configuration

The project uses `pydantic-settings` for configuration management (src/knack_elt/config.py:1). Required environment variables:
- `motherduck_api_key`: MotherDuck API key for destination database
- `knack_api_key`: Knack REST API key for authentication
- `knack_app_id`: Knack application ID

## Architecture Overview

### Data Flow
1. **Extract**: RESTClient fetches data from Knack API with pagination (src/knack_elt/knack_dlt.py:21-37)
2. **Map**: Field mappings convert Knack field IDs to human-readable names (src/knack_elt/mapping.py:14-60)
3. **Transform**: dlt transformers clean data and apply field remappings (src/knack_elt/knack_dlt.py:132-156)
4. **Load**: Data is loaded to MotherDuck using SCD2 merge strategy (src/knack_elt/knack_dlt.py:199-211)

### Key Components

**knack_dlt.py**: Main pipeline orchestration
- `knack_ave_source()`: dlt source that creates resources for all Knack objects (src/knack_elt/knack_dlt.py:159)
- `get_knack_table_data()`: dlt resource factory that yields paginated records from Knack API (src/knack_elt/knack_dlt.py:66)
- `get_remap_transformer()`: dlt transformer factory that remaps field IDs to names (src/knack_elt/knack_dlt.py:132)
- Pipeline uses SCD2 (Slowly Changing Dimension Type 2) merge strategy with "id" as primary key

**mapping.py**: Field and object mapping logic
- `create_app_mappings()`: Parses Knack metadata to create bidirectional field/object mappings (src/knack_elt/mapping.py:14)
- Returns field_mappings (object_id -> field_key -> slugified_name), object_mappings (table_name -> object_id), numeric_fields list, and default_values dict
- Handles restricted field names like 'id' by prefixing with object singular name (src/knack_elt/mapping.py:44-45)
- Slugifies field names using regex to convert to lowercase snake_case (src/knack_elt/mapping.py:46)

**config.py**: Settings management
- Uses pydantic-settings BaseSettings for environment variable loading
- Note: Missing import for `os` module (used on line 5)

### Data Processing Details

**Resource/Transformer Chaining**: Resources are chained with transformers using the pipe operator: `table_resource | transformer_resource` (src/knack_elt/knack_dlt.py:182)

**Write Disposition**: All resources use merge with SCD2 strategy, tracking historical changes (src/knack_elt/knack_dlt.py:68, 134)

**Data Cleaning**:
- Empty strings in numeric fields converted to None (src/knack_elt/knack_dlt.py:99)
- Default values assigned to boolean fields (src/knack_elt/knack_dlt.py:108)
- JSON fields validated and cleaned (src/knack_elt/knack_dlt.py:116)

**Field Type Tracking**: Numeric field types include number, currency, link, date_time, auto_increment, count (src/knack_elt/mapping.py:48)

### dlt Configuration

- Pipeline runs with 3 load workers (src/knack_elt/knack_dlt.py:194)
- Staging dataset truncation enabled (src/knack_elt/knack_dlt.py:195)
- Nesting disabled (`max_table_nesting=0`) to keep flat table structure (src/knack_elt/knack_dlt.py:159)
- Schema export path: `pipelines/schemas/export` (src/knack_elt/knack_dlt.py:208)
- Pipeline saves load_info and trace to destination tables `_load_info` and `_trace` (src/knack_elt/knack_dlt.py:229-232)
