# KnackELT Architecture

How knack-elt works, and where it sits in a full Knack → warehouse → BI stack.

Four diagrams:

1. [Reference architecture](#1-reference-architecture) — the end-to-end stack,
   in [plain language](#the-stack-in-plain-language) and with [the technical detail](#the-same-stack-with-the-technical-detail)
2. [Pipeline flow](#2-pipeline-flow) — what knack-elt actually does on a run
3. [Run sequence](#3-run-sequence) — the call order for one invocation
4. [SCD2 row lifecycle](#4-scd2-row-lifecycle) — how history is represented, and how to query it

> **Scope note.** This repo is the **ingestion** piece only. The dbt models, BI
> configuration, and CI orchestration shown in diagram 1 are drawn from a working
> production deployment where they live in a companion application repo (whose
> `pipelines/knack_dlt.py` is the production twin of this repo's pipeline). They are
> real, not aspirational — but they are not in this tree. Subgraph labels mark what
> lives where.
>
> An adjacent, separate consumer path (LanceDB / RAG / chat) is described in
> [RAG_ARCHITECTURE.md](RAG_ARCHITECTURE.md) and is deliberately not folded in here.

---

## 1. Reference architecture

Knack is the system of record and the operational UI. It is not a warehouse: no
SQL, no aggregates, no history, and a record cap that eventually forces pruning.
The stack below turns it into one — MotherDuck as the warehouse, dbt as the
transformation layer, Preset (managed Apache Superset) as the access layer.

Same architecture, drawn twice: once for a stakeholder audience, once with the
detail an engineer needs.

### The stack in plain language

```mermaid
flowchart TB
    subgraph work["Where the work happens"]
        knack["<b>Knack</b><br/>the app the team uses every day<br/>bookings · invoices · people · payments"]
    end

    subgraph copyout["Getting the data out"]
        pipe["<b>knack-elt</b><br/>a nightly, automatic copy<br/>keeps every version of every record<br/>and never overwrites the old one"]
    end

    subgraph wh["The data warehouse — MotherDuck"]
        raw[("<b>Complete history</b><br/>every record Knack has ever had —<br/>including records later deleted there")]

        subgraph rep["Reporting tables — prepared by <b>dbt</b>"]
            clean["<b>Cleaned up</b><br/>dates, dollar amounts and linked records<br/>put into a form you can add up and sort"]
            biz["<b>Business views</b><br/>payroll · events · people · invoices<br/>shaped around the questions people ask"]
            clean --> biz
        end
    end

    subgraph answers["Where people get answers — Preset"]
        dash["<b>Dashboards</b><br/>look up a person, an event, a payment —<br/>including ones no longer in Knack"]
        adhoc["<b>Ad-hoc questions + export</b><br/>new questions without a developer<br/>results out to Excel or CSV"]
    end

    dr[("<b>OPTIONAL — offsite backup</b><br/>S3-compatible object storage<br/>AWS S3 · Cloudflare R2 · Backblaze B2 · MinIO<br/>a dated copy kept outside the warehouse,<br/>for disaster recovery only")]

    knack -->|"read-only, nightly"| pipe
    pipe --> raw
    raw --> clean
    biz --> dash
    biz --> adhoc
    raw -.->|"scheduled backup<br/>(optional)"| dr

    classDef optional fill:#fafafa,stroke:#9ca3af,stroke-width:2px,stroke-dasharray: 6 4
    class dr optional
```

**How to read it.** The copy runs one way only — nothing this stack does can change
data in Knack. The warehouse keeps history, so a record deleted in Knack is still
answerable afterwards; that is the whole point of having it. The two boxes inside
the warehouse are a division of labour, not two copies of the data: the complete
history is what arrives, and **dbt** is the tool that turns it into the cleaned,
business-shaped tables people actually query.

The dashed box is genuinely optional. The warehouse is already a second copy of
Knack, so this is a third — insurance against losing the *warehouse* (a bad
migration, an account problem, a provider outage), not against losing Knack. Add
it when the warehouse becomes the only remaining copy of pruned records; skip it
while Knack still holds everything. Any S3-compatible store works, and retention
is normally handled by the bucket's own lifecycle rules rather than by code.

### The same stack, with the technical detail

```mermaid
flowchart TB
    subgraph src["Source of record"]
        knack["Knack App<br/>low-code UI + database"]
        kapi["Knack REST API<br/>api.knack.com/v1"]
        knack --- kapi
    end

    subgraph elt["Ingestion — knack-elt (THIS REPO)"]
        pipe["dlt pipeline<br/>extract · normalize · load<br/>merge, strategy = scd2<br/>primary key = id"]
    end

    subgraph md["Warehouse — MotherDuck (DuckDB cloud)"]
        raw[("raw schema — dlt writes here<br/>e.g. knack_avondale_data.avondale<br/>one table per Knack object<br/>full SCD2 history")]
        obs[("_load_info · _trace<br/>pipeline observability")]

        subgraph rep["reporting schema — the tables people query<br/>defined and built by dbt (companion repo)"]
            stg["staging views (stg_*)<br/>one per source table<br/>SCD2 flags · currency + date casts<br/>connection-array extraction"]
            marts["mart views<br/>consumer-facing, documented<br/>live / deleted posture"]
            stg --> marts
        end
    end

    subgraph bi["Access layer"]
        preset["Preset<br/>managed Apache Superset"]
        lab["SQL Lab<br/>ad-hoc + query development"]
        dash["Dashboards<br/>lookup · drill-down · analytics"]
        exp["CSV / Excel export"]
        preset --> lab
        preset --> dash
        dash --> exp
    end

    kapi -->|"paginated REST<br/>1000 rows/page"| pipe
    pipe --> raw
    pipe --> obs
    raw -->|"dbt run"| stg
    marts -->|"SQL over MotherDuck"| preset

    cron["daily cron — GitHub Actions<br/>(companion repo)<br/>1. dlt sync, one retry on failure<br/>2. dbt build + data tests<br/>3. offsite Parquet export (optional)"]
    s3[("OPTIONAL — disaster recovery<br/>S3-compatible object storage<br/>AWS S3 · Cloudflare R2 · Backblaze B2 · MinIO<br/>dated Parquet snapshots, lifecycle-managed")]

    cron ==>|"triggers"| pipe
    raw -.->|"scheduled Parquet export<br/>(optional)"| s3

    classDef here fill:#dbeafe,stroke:#1d4ed8,stroke-width:3px
    classDef optional fill:#fafafa,stroke:#9ca3af,stroke-width:2px,stroke-dasharray: 6 4
    class elt here
    class s3 optional
```

**The load-bearing detail: schema ownership is split.**

| Schema | Owner | Contents | Rule |
|---|---|---|---|
| raw (e.g. `avondale`) | dlt | One table per Knack object, full SCD2 history, Knack-shaped values | **dbt never builds into it.** It is the pipeline's output, and a `dbt run` writing here would be clobbered on the next sync. |
| `reporting` | dbt | Staging + mart views | Everything downstream reads here. No dashboard queries raw directly. |

Staging exists so that exactly one layer absorbs the mess Knack and dlt produce
together — currency strings with commas, dates wrapped in JSON, connection fields
as JSON arrays, and dlt's type-variant columns (when a value doesn't fit the type
inferred from the first batch, dlt NULLs the base column and diverts the value to
a sibling like `amount__v_double`, which makes a naive `SUM()` under-report
silently). Marts and dashboards then get to be simple.

**Why Preset:** it is managed Superset, so SQL Lab doubles as the query-development
environment and the dashboard builder against the same MotherDuck connection —
a proven query becomes a dataset becomes a chart with no rewrite. Any BI tool with
a DuckDB/MotherDuck SQLAlchemy connection substitutes here; the contract is the
`reporting` schema, not the tool. Note that row-level security and embedded
analytics are paid-tier features in Preset — untrusted multi-tenant self-service
needs a different surface (a scoped API in front of MotherDuck), not a BI seat.

---

## 2. Pipeline flow

What one run of knack-elt does. Two inputs from Knack: the **app metadata**
(schema) and the **records** (data). The metadata is what turns `field_73` into
`payroll_name`.

```mermaid
flowchart TB
    start(["knack-elt run-pipeline"]) --> meta

    subgraph mapping["Schema pass — mapping.py"]
        direction TB
        meta["Knack app metadata<br/>GET /applications/{app_id}<br/>via knack-sleuth"]
        cam["create_app_mappings()"]
        meta --> cam
        cam --> om["object_mappings<br/>object name → object_id"]
        cam --> fm["field_mappings<br/>object_id → field_key → slug"]
        cam --> nf["numeric_fields<br/>number · currency · link · date_time<br/>auto_increment · count"]
        cam --> dv["default_values<br/>boolean field defaults"]
    end

    om --> loop

    subgraph build["Resource build — knack_dlt.py"]
        loop{{"for each Knack object"}}

        subgraph resource["dlt.resource — get_knack_table_data()"]
            direction TB
            pg["client.paginate<br/>/objects/{object_id}/records<br/>rows_per_page=1000 · format=raw"]
            stamp["stamp table_name + object_id<br/>onto every row"]
            pk{"row has an id?"}
            skip["log warning, drop row"]
            cjf["clean_json_fields()<br/>invalid or empty JSON → None"]
            pg --> stamp --> pk
            pk -->|no| skip
            pk -->|yes| cjf
        end

        subgraph transformer["dlt.transformer — get_remap_transformer()"]
            direction TB
            ces["clean_empty_strings()<br/>empty string → None<br/>in numeric fields"]
            adv["assign_default_values()<br/>None or empty → declared default"]
            rk["remap_keys()<br/>field_43 → event_name"]
            ces --> adv --> rk
        end

        loop --> pg
        cjf -->|"chained with the pipe operator"| ces
    end

    nf -.-> ces
    dv -.-> adv
    fm -.-> rk

    rk --> norm

    subgraph load["Load — dlt"]
        direction TB
        norm["normalize<br/>max_table_nesting=0 → flat tables"]
        merge["merge, strategy = scd2<br/>primary_key = id"]
        dest[("destination<br/>DuckDB or MotherDuck")]
        info["load_info + trace<br/>→ _load_info / _trace tables"]
        norm --> merge --> dest --> info
    end
```

**Notes on the mapping pass**

- Field names are slugified with `re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')`,
  so `"Vocalist / Song Details"` becomes `vocalist_song_details`.
- A user-defined Knack field literally named `id` would collide with Knack's own
  record id, so it is renamed to `<singular>_id` (e.g. `worker_id`). Knack's row id
  is always `id`, and it is the pipeline's primary key.
- `numeric_fields` and `default_values` are registered under the field key, the raw
  name, **and** the slug, because cleaning runs before and after the remap.
- `remap_keys` falls back to the original key when no mapping exists, so an unmapped
  object still loads — with `field_NN` column names.

---

## 3. Run sequence

The packaged entry point is the Typer CLI (`knack-elt run-pipeline`). It takes the
app metadata from knack-sleuth rather than fetching it itself.

```mermaid
sequenceDiagram
    autonumber
    actor Dev as Operator / CI
    participant CLI as cli.py
    participant Sleuth as knack-sleuth
    participant Src as build_knack_resources
    participant Knack as Knack REST API
    participant Trf as remap transformer
    participant DLT as dlt pipeline
    participant Dest as DuckDB / MotherDuck

    Dev->>CLI: knack-elt run-pipeline --app-id APP
    CLI->>Sleuth: load_app_metadata(app_id)
    Sleuth->>Knack: GET /applications/{app_id}
    Knack-->>Sleuth: app metadata
    Sleuth-->>CLI: Application model

    Note over CLI: derive names from app slug:<br/>db knack_{slug}_data<br/>dataset {slug}<br/>(hyphens → underscores)<br/>pipeline knack_{slug}_pipeline

    CLI->>DLT: dlt.pipeline(name, dataset, destination)
    CLI->>Src: build_knack_resources(kn_app)
    Src->>Src: create_app_mappings(kn_app)

    loop per Knack object
        Src->>Src: chain resource into transformer
    end
    Src-->>DLT: list of chained resources

    CLI->>DLT: pipeline.run(source)

    loop per resource (dlt extracts concurrently)
        loop per page
            DLT->>Knack: GET /objects/{oid}/records?page=N
            Knack-->>DLT: records + total_pages
            DLT->>Trf: row
            Trf-->>DLT: cleaned, remapped row
        end
    end

    DLT->>DLT: normalize (flat, max_table_nesting=0)
    DLT->>Dest: load — merge / scd2 on id
    Dest-->>DLT: load_info
    DLT-->>CLI: load_info
    CLI-->>Dev: run summary
```

**Two entry points, and they do not target the same destination today:**

| Entry point | Metadata source | Object list | Destination |
|---|---|---|---|
| `cli.py run_pipeline` (`knack-elt run-pipeline`) | knack-sleuth `load_app_metadata()` | every object in the app | **local DuckDB** at `tests/data/knack_{slug}_data.duckdb` — the MotherDuck destination is present but commented out (`cli.py:80-84`, see the TODO at `cli.py:68`) |
| `python -m knack_elt.knack_dlt` (`knack_ave_source`) | its own HTTP `GET /applications/{id}` | every object in the app | **MotherDuck**, database name hardcoded to `knack_avondale_data` |

The `__main__` path is the older one; it also sets `load.workers = 3`,
`truncate_staging_dataset = True`, exports the schema to `pipelines/schemas/export`,
and writes `load_info` and the run trace back to the destination as `_load_info`
and `_trace`. Making the destination a CLI flag is the obvious next step and the
TODO already names it.

---

## 4. SCD2 row lifecycle

Every table is loaded with `write_disposition={"disposition": "merge", "strategy": "scd2"}`,
so the warehouse is **append-and-retire, not overwrite**. dlt adds `_dlt_valid_from`
and `_dlt_valid_to` to every row. A record's history is therefore several rows sharing
one `id`.

```mermaid
flowchart TB
    subgraph run1["Sync 1 — record first seen"]
        a1["id=abc · status='Booked'<br/>_dlt_valid_from = T1<br/>_dlt_valid_to = NULL"]
    end

    subgraph run2["Sync 2 — value changed in Knack"]
        b1["id=abc · status='Booked'<br/>_dlt_valid_from = T1<br/>_dlt_valid_to = T2 ← retired"]
        b2["id=abc · status='Confirmed'<br/>_dlt_valid_from = T2<br/>_dlt_valid_to = NULL ← current"]
    end

    subgraph run3["Sync 3 — record deleted from Knack"]
        c1["id=abc · status='Booked'<br/>valid T1 → T2"]
        c2["id=abc · status='Confirmed'<br/>_dlt_valid_from = T2<br/>_dlt_valid_to = T3<br/>retired with NO successor"]
    end

    run1 --> run2 --> run3

    run3 --> deleted_note["The record no longer exists in Knack.<br/>It exists in the warehouse ONLY as a retired row.<br/>This is the point of the warehouse — but it means<br/>'_dlt_valid_to IS NULL' silently drops archived records."]
```

Because of that last case, two different flags are needed, and conflating them is
the most common way to get a wrong answer out of this stack:

```sql
-- one row per record, live or deleted, plus the live/deleted flag
with flagged as (
    select
        *,
        row_number() over (partition by id order by _dlt_valid_from desc) = 1
            as latest_version,
        _dlt_valid_to is null as is_live_in_knack
    from avondale.some_table          -- the raw, dlt-owned schema
)
select * from flagged where latest_version
```

| Question | Filter |
|---|---|
| Current state of everything still in Knack | `latest_version and is_live_in_knack` |
| Latest known state of every record ever seen, including deleted | `latest_version` |
| What did this record look like on a given date | `_dlt_valid_from <= d and (_dlt_valid_to is null or _dlt_valid_to > d)` |
| What has been deleted from Knack | `latest_version and not is_live_in_knack` |

Two traps worth stating explicitly:

- **Partition by `id`, not by a Knack field that looks like an id.** `id` is the
  canonical Knack record id supplied by the API and is populated on every row. A
  user-added "Record ID" *field* is NULL on rows created before it existed, and
  partitioning on it collapses all of them into one NULL group and corrupts the flags.
- **Aggregate without `latest_version` and you double-count.** Every historical
  version of a row is still a row. A `SUM()` over the raw table sums every version
  of every record.

Deleted-in-Knack is not the same as *archived by policy* — distinguishing an
intentional retention prune from incidental deletion needs a separate manifest of
what the pruning process removed; SCD2 alone cannot tell them apart.

---

## Configuration

`pydantic-settings`, loaded from environment or `.env` (`src/knack_elt/config.py`):

| Variable | Used for |
|---|---|
| `KNACK_APP_ID` | Knack application id — also the default for `--app-id` |
| `KNACK_API_KEY` | Knack REST API key (sent as `X-Knack-REST-API-Key`) |
| `motherduck_api_key` | MotherDuck token, interpolated into the `md:///` connection string |

Note that dbt-duckdb reads the MotherDuck token from `motherduck_token`, not
`motherduck_api_key` — the two layers disagree on the variable name, so a
deployment running both needs to set or shim both.
