# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KnackELT is a Python ELT (Extract, Load, Transform) pipeline that extracts data from a Knack
application and loads it into DuckDB or MotherDuck using the dlt (data load tool) framework.
It is the **generic, licensed** extraction of a client-specific pipeline — nothing in `src/`
may hardcode an app, slug, object key, or field name. App metadata (objects, fields,
inflections) comes from the `knack-sleuth` package.

## Development Setup

This project uses `uv` as the package manager.

```bash
uv sync
uv run pytest tests/ -q          # offline; never hits the Knack API
uv run ruff check src/ tests/
```

### Running the Pipeline

Packaged as a Typer CLI (`[project.scripts]` → `knack_elt.cli:cli`) and published on PyPI as
`knack-elt`. In this repo, work through `uv run`; the README documents the uvx/pip/clone
routes for users.

```bash
uv run knack-elt run-pipeline --app-id <knack_app_id>                    # local DuckDB file
uv run knack-elt run-pipeline --app-id <id> --destination motherduck     # MotherDuck
```

`--destination local` is the **default and deliberate**: a fresh install must be able to load a
Knack app with no MotherDuck account. The file goes to `default_db_dir()` —
`$XDG_DATA_HOME/knack-elt/`, falling back to `~/.local/share/knack-elt/` — unless `--db-path`
overrides it, and the resolved path is printed each run. **Do not make this CWD-relative
again**: the same app must keep one warehouse wherever the command runs, or a record's SCD2
history silently splits across directories. Two tests pin it.

Other flags: `--api-key`, `--refresh-metadata` (bypass knack-sleuth's 24h metadata cache),
`--skip-unreadable` (see the SCD2 warning below).

**Naming** — everything derives from the app slug resolved via `knack_sleuth.load_app_metadata()`:
database `knack_{slug}_data`, pipeline `knack_{slug}_pipeline`, dataset `{slug}` with dashes
replaced by underscores.

## Configuration

`pydantic-settings` reads the environment and a `.env` file (`src/knack_elt/config.py`):
- `KNACK_APP_ID` — Knack application id (pydantic `Field` alias); default for `--app-id`
- `KNACK_API_KEY` — Knack REST API key (alias); default for `--api-key`
- `motherduck_api_key` — required only for `--destination motherduck`

The CLI validates that an app id and API key are present before doing any work.

## Architecture Overview

### Data Flow
1. **Metadata**: `load_app_metadata(app_id=...)` returns a validated knack-sleuth `Application`
2. **Extract**: `create_rest_client(app_id, api_key)` builds a `RESTClient`; one dlt resource per
   Knack object pages records at 1000/page in `format=raw`
3. **Map**: `create_app_mappings()` turns Knack field keys into slugified column names
4. **Transform**: one dlt transformer per object cleans values, then remaps keys
5. **Load**: merge with the SCD2 strategy, keyed on `record_id`

The app id and API key are threaded from the CLI all the way into the record client. Do not
reintroduce reads of `settings.*` inside `knack_dlt.py` — that is how metadata and records
previously ended up pointing at different apps.

### Key Components

**knack_dlt.py** — generic pipeline building blocks
- `build_knack_resources(kn_app, client, skip_unreadable=False)`: `@dlt.source(max_table_nesting=0)`
  yielding one `resource | transformer` pair per Knack object
- `get_knack_table_data()`: dlt resource factory; resource named `table_{object_key}`
- `get_remap_transformer()`: dlt transformer factory; resource named `remap_{object_key}`,
  destination table set via `table_name=`
- `LINEAGE_TABLE_NAME` / `LINEAGE_OBJECT_ID` (`_kn_table_name`, `_kn_object_id`): stamped on every
  row. Underscore-prefixed on purpose — a Knack field named "Table Name" slugifies to `table_name`
  and would otherwise be overwritten by the pipeline's own bookkeeping.
- `RECORD_KEY` (`record_id`): the merge key. Knack's row id arrives as the payload's top-level
  `id` and is renamed in the resource before anything else touches the row. **Take it from the
  payload, never from Knack's auto-added "Record ID" field** — the payload key exists on every
  app, the field only on ones Knack has migrated.

**mapping.py** — field and object mapping
- `create_app_mappings(app_metadata: Application)`: takes a knack-sleuth `Application` (not raw
  API JSON) and returns `field_mappings` (object_id → field_key → slug), `object_mappings`,
  `numeric_fields`, `default_values`
- `slugify_field_name()`: `re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')` — **can return `""`**
- Collision handling: two legal Knack field names can slugify identically (`"Total ($)"` and
  `"Total (%)"` → `total`), and a name with no ASCII alphanumerics slugifies to `""`. Either
  collapses columns in `remap_keys`, where the last field silently wins, so a colliding or empty
  slug falls back to the field key (`total_field_12`) and logs a warning. Keep it that way.
- `restricted_field_names` reserves `record_id` only. `id` is deliberately **not** reserved:
  since the row id loads as `record_id`, a user field named "ID" keeps the `id` column it was
  named for (three AV/E objects rely on this). Knack auto-adds a `short_text` "Record ID" field
  to every object, which slugifies onto the merge key, so it is renamed `<singular>_record_id`.
  Reservation compares the **slug**, not the raw name — `"Record ID".lower()` is `"record id"`.
- `column_name_for_field()` resolves every column name. Every fallback is **re-checked**, not
  trusted: the reserved-name escape (`<singular>_record_id`) can slugify straight back to
  `record_id` when the singular has no ASCII alphanumerics, and the collision escape
  (`{slug}_{field_key}`) is a name a field can already hold.
- `NUMERIC_FIELD_TYPES` drives empty-string→NULL cleaning: `number`, `currency`, `link`,
  `date_time`, `auto_increment`, `count`, `sum`, `min`, `max`, `average`, `equation`, `rating`.
  Omitting a type means that column types as VARCHAR full of `''` — add new types here.

**cli.py** — Typer app: `--version` plus `run-pipeline`

### Data Processing Details

**Chaining**: `table_resource | transformer_resource` (pipe operator).

**Write disposition**: `{"disposition": "merge", "strategy": "scd2"}`, `primary_key=RECORD_KEY`, on
both resource and transformer. `columns={RECORD_KEY: {"merge_key": False}}` works around a suspected
dlt bug.

**Cleaning order**: cleaning runs **before** the remap, so `numeric_fields` and `default_values`
are keyed on raw Knack field keys *only*. Do not re-register them under the slug or raw name —
those entries can never match a row. There is deliberately no JSON-string cleaning: `format=raw`
returns rich fields as dicts and `max_table_nesting=0` leaves dlt to serialise them.

**Duplicate table names**: `destination_table_name()` dedupes on the name **dlt will actually
create**, not the raw Knack object name — dlt snake_cases it afterwards, so `Order Items` and
`order-items` both become `order_items`, and any name without ASCII alphanumerics becomes `x`.
A shared table is not cosmetic: each run's SCD2 merge retires the other object's rows as
deleted-in-Knack. It falls back to the object key, and rejects names that normalize to empty
or to a leading underscore (dlt owns that prefix).

### SCD2 — two traps

1. `_dlt_valid_to IS NULL` means "still present in Knack". Filtering on it alone silently drops
   records deleted upstream, which are exactly the ones the warehouse exists to keep. Partition
   history by `record_id` — not `id` (may be an app's own numbering) and not
   `<singular>_record_id` (a user-editable copy).
2. **A partial extraction is worse than a failed one.** If a resource yields some rows and then
   errors, dlt sees a successful load and the merge retires every row missing from that partial
   batch — silent history corruption. This is why `--skip-unreadable` only swallows failures that
   occur *before any row is yielded*; a mid-stream failure always re-raises. Preserve that
   invariant in any error-handling change.

Known gap: an object that returns zero records produces no load package at all, so previously
loaded rows stay marked live. Emptying an object in Knack is invisible to the flag.

## Testing

`tests/test_pipeline_offline.py` runs the real source against a `FakeClient` and synthetic
`Application` fixtures — no network, no credentials. Every case is a shape a fresh Knack app can
legally have (colliding slugs, non-Latin field names, fields named "Table Name", duplicate object
names, unreadable objects, mid-stream failures). Add regressions here rather than reaching for a
live app id.

## Documentation

`docs/ARCHITECTURE.md` is the architecture reference — four diagram sections, five ```mermaid
blocks (section 1 has two). `docs/ARCHITECTURE.pdf` is a build artifact of it: regenerate with
`uv run scripts/build_architecture_pdf.py` (needs node and Chrome; correct output is 12 pages)
whenever the markdown changes. When editing any mermaid block — in `docs/ARCHITECTURE.md` or
`README.md`, which has one of its own — render it with mermaid-cli before committing; parse
errors and unreadable layouts are invisible in the markdown source.

Docs are public-facing for a licensed repo: use generic example names (`acme_ops`, `{slug}`),
never a real client's app, schema, or field names.
