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

**Naming** — physical identities never derive from editable labels. `stable_app_identifier()`
derives the database, pipeline and dataset from the immutable app id; object tables use
`object_N`; field columns use `field_N`. Current labels live in `_kn_object_catalog` and
`_kn_field_catalog`.

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
3. **Map**: `create_app_mappings()` keeps immutable Knack field keys as physical columns
4. **Transform**: one dlt transformer per object cleans values, then remaps keys
5. **Load**: merge with the SCD2 strategy, keyed on `record_id`

The app id and API key are threaded from the CLI all the way into the record client. Do not
reintroduce reads of `settings.*` inside `knack_dlt.py` — that is how metadata and records
previously ended up pointing at different apps.

### Key Components

**knack_dlt.py** — generic pipeline building blocks
- `build_knack_resources(kn_app, client, skip_unreadable=False, extraction_status=None)`:
  `@dlt.source(max_table_nesting=0)`
  yielding one `resource | transformer` pair per Knack object
- `get_knack_table_data()`: dlt resource factory; resource named `table_{object_key}`
- `get_remap_transformer()`: dlt transformer factory; resource named `remap_{object_key}`,
  destination table set via `table_name=`
- `LINEAGE_TABLE_NAME` / `LINEAGE_OBJECT_ID` (`_kn_table_name`, `_kn_object_id`): stamped on every
  row. Underscore-prefixed on purpose; user fields remain under `field_N` keys.
- `RECORD_KEY` (`record_id`): the merge key. Knack's row id arrives as the payload's top-level
  `id` and is renamed in the resource before anything else touches the row. **Take it from the
  payload, never from Knack's auto-added "Record ID" field** — the payload key exists on every
  app, the field only on ones Knack has migrated. A row arriving without `id` raises
  `MalformedRecord`: skipping it would shrink the batch, and when the envelope omits
  `total_records` the shortfall check cannot catch what the skip removed.

**mapping.py** — field cleaning metadata and stable mapping
- `create_app_mappings(app_metadata: Application)`: takes a knack-sleuth `Application` (not raw
  API JSON) and returns `field_mappings` (object_id → field_key → field_key), `object_mappings`,
  `numeric_fields`, `default_values`
- Labels are deliberately not physical identifiers. This prevents label renames and collisions
  with top-level Knack user-system keys such as `account_status` from dropping data.
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

**Stable table names**: `destination_table_name()` always returns the immutable object key.
Duplicate, renamed and non-Latin labels cannot change or collide with physical tables.

### SCD2 — two traps

1. `_dlt_valid_to IS NULL` means "still present in Knack". Filtering on it alone silently drops
   records deleted upstream, which are exactly the ones the warehouse exists to keep. Partition
   history by `record_id` — not `id` (may be an app's own numbering) and not
   `<singular>_record_id` (a user-editable copy).
2. **A partial extraction is worse than a failed one.** If a resource yields some rows and then
   errors, dlt sees a successful load and the merge retires every row missing from that partial
   batch — silent history corruption. `--skip-unreadable` only swallows an HTTP 403 before any
   row is yielded; authentication, rate-limit, network, server and mid-stream failures re-raise.

After a successful load, `reconcile_scd2_tables()` explicitly closes live rows for objects that
reported zero records or disappeared from metadata. It is scoped to the pipeline's own
dataset; label-named tables from older releases live in another database entirely and are
left alone.

Known source limitation, documented in the README:
- Knack's record API pages by number, not cursor, so a record inserted or deleted mid-extraction
  shifts page boundaries and can be missed from that batch. **Detected, not prevented**: each page
  envelope carries `total_records`, and `get_knack_table_data` raises `RecordCountShortfall` when
  fewer records arrive than Knack reported throughout, which aborts before the merge can retire
  them. The comparison floor is `min(first_total, last_total)` — the count moves under us, and the
  lower endpoint is what we can be sure was present the whole time. This is best-effort: equal-count
  churn can evade the check, so production runs should target quiet source periods.
  `--skip-unreadable` explicitly
  does **not** swallow this. If the envelope omits the count, reconciliation is skipped rather
  than failing closed.
- **Rejected: sorting the extraction.** `sort_field=<Record ID field key>&sort_order=asc` does
  work (verified against the live API — `sort_field=id` is silently ignored; it must be the field
  key of Knack's auto-added "Record ID" field, which all 90 objects across the three known apps
  have exactly one of). It closes only *half* the window and costs too much for that:
  - **Insertions**: closed. New records get higher ObjectIds, so with `asc` they append past the
    read cursor and already-read pages never shift. (`desc` is also safe — new rows land on page 1
    and records slide *forward*, producing duplicate reads the merge dedupes on `record_id` — but
    it re-reads rows for nothing, so `asc` is the better of the two.)
  - **Deletions**: NOT closed, in either direction. A record deleted before the cursor pulls
    everything after it back one slot, so a row at the top of page N+1 slides to the bottom of
    page N, which is already read. This is exactly the case the count check catches.
  - **Cost, measured 2026-08-26** on a 7,568-record object at 1000 rows/page: full sweep 33.2s
    unsorted vs 43.5s sorted, median of 2 trials, slower in both — **+31%**. Per-page timings were
    pure noise; only the full sweep is meaningful. Across a 61-object app that is a third added to
    every sync to reduce how often a check that already exists has to fire.

  Do not re-propose this without a new measurement. If it is ever revisited, the argument that
  would change the answer is Knack shipping a cursor API, not a faster sort.

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

Docs are public-facing for a licensed repo: use generic example names (`acme_ops`,
`{stable_app_id}`),
never a real client's app, schema, or field names.
