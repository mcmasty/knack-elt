# Label views: matching the warehouse to current Knack labels

**Date:** 2026-08-26
**Status:** Approved design, not yet implemented

## Problem

Physical warehouse identities are immutable by design: tables are `object_N`, columns are
`field_N`, both derived from Knack keys that never change. That is what keeps a record's SCD2
history intact when someone renames a table in the Knack builder from Courses to Classes.

The cost is that nobody can read the warehouse. An analyst looking for Classes finds
`object_3`, and its columns are `field_1 … field_47`. The current labels are already loaded
into `_kn_object_catalog` and `_kn_field_catalog` on every sync, but joining against a catalog
to name a column is not a workflow anyone will adopt.

We want the warehouse to be browsable under the names the Knack builder shows, without any
label ever becoming a physical identity.

## Non-goals

- **Renaming physical tables or columns.** Rejected outright. It reintroduces every failure
  the stable-identity design exists to prevent: label collisions become table collisions, a
  label of `record_id` clobbers the merge key, and a rename that fails halfway leaves a mixed
  schema. See CLAUDE.md, "Naming".
- **Automatic propagation.** A rename in the Knack builder must never move a warehouse name on
  its own. See "Why triggered, not automatic" below.
- **Views over `_load_info`, `_trace`, or the catalogs.** Internal bookkeeping stays internal.
- **A MotherDuck-specific code path.** The SQL is identical. Like the rest of that destination,
  it is verified by reading rather than by running; the one place this matters is noted below.

## Why triggered, not automatic

The obvious design is to rebuild the views at the end of every `run-pipeline`, so they always
match Knack. That is wrong, and for the same reason the rest of this project distrusts labels.

A Knack app is edited by non-engineers through a form builder. If views rebuild automatically,
someone renaming a table on a Tuesday afternoon silently renames the analyst's view and breaks
every dashboard pointing at the old name — with no human in the loop, no announcement, and no
way to see it coming. The blast radius of a label edit becomes production.

So the view layer is **derived but triggered**: it is always a pure function of the catalog
(there is no mapping state to drift), but nothing moves until someone runs the command and
confirms the plan. `run-pipeline` reports drift and does nothing about it — the same
"detected, not prevented" posture the pipeline already takes toward the pagination gap.

## Architecture

```
{stable_app_id}              physical, managed by dlt, never renamed
  object_1, object_3, …      SCD2 tables
  _kn_object_catalog         object_id -> object_name
  _kn_field_catalog          object_id, field_key -> field_name
  _dlt_loads, …

{stable_app_id}_labels       views only, managed by knack-elt, disposable
  "Classes"                  live rows, columns aliased to current labels
  "Classes_history"          every version, plus validity columns
```

A separate schema is what makes the layer safe. A Knack label can be any text, including
literally `object_1`, `record_id`, or `_kn_object_catalog`. Isolating the views means a label
can never collide with a physical name — the collision is structurally impossible rather than
defended against.

It also makes the layer disposable: everything in `{stable_app_id}_labels` is generated, so it
can be dropped and rebuilt wholesale without touching data.

### Source of truth

Views are built from `_kn_object_catalog` and `_kn_field_catalog` **read out of the warehouse**,
never from a live Knack `Application`. The command therefore needs no API key, no network, and
no metadata fetch, and works against any existing warehouse.

The consequence to state plainly: views reflect labels **as of the last sync**, not as of this
moment. Someone who renames in the builder and wants the warehouse to match runs a sync and
then `refresh-views` — which is the order they would do it in anyway.

## New module: `src/knack_elt/labels.py`

No Knack dependency; it reads the warehouse and writes views.

```python
def plan_label_views(pipeline) -> LabelViewPlan
def apply_label_views(pipeline, plan: LabelViewPlan) -> LabelViewReport
def describe_drift(pipeline) -> DriftReport
```

`LabelViewPlan` holds the target view set plus the diff against what currently exists:
created, renamed, dropped, and skipped. It is inert — computing a plan writes nothing.

### View shape

Two views per object. Both are generated; there is no flag to suppress either.

```sql
CREATE OR REPLACE VIEW {labels}."Classes" AS
SELECT record_id,
       field_1 AS "Name",
       field_2 AS "Start Date"
FROM {data}.object_3
WHERE _dlt_valid_to IS NULL;

CREATE OR REPLACE VIEW {labels}."Classes_history" AS
SELECT record_id,
       field_1 AS "Name",
       field_2 AS "Start Date",
       _dlt_valid_from      AS valid_from,
       _dlt_valid_to        AS valid_to,
       _dlt_valid_to IS NULL AS is_live_in_knack
FROM {data}.object_3;
```

Both views are stamped with `COMMENT ON VIEW … IS '<object_key>'`, which is how a later run
attributes an existing view to its object.

The split exists because "current" has two meanings under SCD2 and conflating them is
CLAUDE.md's first trap. `Classes` answers "what is in Knack now" and is what a BI tool should
point at. `Classes_history` keeps the records deleted upstream — exactly the rows the warehouse
exists to preserve — visible under a name that says what they are, instead of silently absent.

Lineage and dlt bookkeeping columns (`_kn_table_name`, `_kn_object_id`, `_dlt_id`,
`_dlt_load_id`) are excluded from both views.

### Naming rules

Nothing is slugified. The whole point is to match what the builder shows, so labels become
quoted identifiers verbatim, with embedded double quotes doubled. Non-Latin labels are kept
as-is; this is verified working in DuckDB.

| Case | Rule |
| --- | --- |
| Two objects labelled `Classes` | **Both** become `Classes__object_3` / `Classes__object_7`. Never one keeps the plain name. |
| A label equals another object's `…_history` name | Base names are reserved in both forms before collisions are computed |
| Empty or whitespace-only label | Falls back to the object key (`object_3`) |
| Two fields in one object share a label | Both suffixed with `__field_N` |
| Field labelled `record_id`, `valid_from`, `valid_to`, `is_live_in_knack` | Suffixed with `__field_N`; passthrough columns always win |

The "both get suffixed" rule is the one that matters. If the first `Classes` kept the plain
name, then adding a second object with the same label would silently move an existing view and
break queries that had nothing to do with the edit. Suffixing both makes a name depend only on
the object it belongs to, never on what else happens to exist.

All four passthrough names are reserved in **both** views, even though `valid_from` and friends
only appear in the history view, so a given field has the same column name in both.

### Columns that do not exist

dlt creates a column only once a value for it has arrived. A Knack field that has never held a
value in any record has no column in the physical table, and generating `field_7 AS "X"` for it
would fail the whole view.

The generator therefore intersects the catalog's fields with the physical table's actual
columns, read from `information_schema.columns`. Catalog fields with no column are omitted from
the view and counted in the report, so "this Knack field is missing from my view" has an answer
rather than being a mystery.

An object in the catalog with no physical table at all (never loaded, or unreadable on every
run) is skipped entirely and reported as skipped.

## Apply is a full rebuild

The plan is a diff, computed for a human to read. The apply is not incremental:

1. `CREATE SCHEMA IF NOT EXISTS {labels}`
2. Drop every view in `{labels}`
3. Create the full target set
4. All of the above in one transaction

`CREATE OR REPLACE` in the SQL above is redundant after step 2 and is kept anyway: it makes each
statement correct in isolation, which matters when one is pasted into a console to debug.

This avoids an ordering hazard that incremental application has and is genuinely hard to get
right: if object_3 is renamed to `Classes` while object_9's view is *currently* named `Classes`,
any create-then-drop order transiently clobbers one of them. A full rebuild has no intermediate
states to reason about, is trivially idempotent, and costs nothing — views are metadata.

Only views are dropped, and only inside `{labels}`. Tables are never touched, and no other
schema is ever read or written. The schema is documented as knack-elt-managed: hand-authored
objects there will be removed.

## CLI

### `knack-elt refresh-views`

```
knack-elt refresh-views --app-id <id> [--destination local|motherduck] [--db-path PATH] [--yes]
```

Resolves the warehouse exactly as `run-pipeline` does — the same `stable_app_identifier()`, the
same `default_db_dir()` — so it addresses the same database without being told twice.

Prints the plan, then asks:

```
Plan for app_xxx_a1b2c3d4_labels:
  ~ "Courses"      -> "Classes"      (object_3)
  ~ "Invoice Line" -> "Line Item"    (object_7)
  + "Cohorts"                        (object_9)
  - "Old Thing"    (object no longer in the catalog)
  ! "Suppliers"    skipped, no physical table

2 renames will break queries using the old names.
Apply? [y/N]
```

`--yes` skips the prompt for scripted use. A plan with no changes reports so and exits 0
without prompting. Exits 1 if any view fails to build.

When stdin is not a terminal and `--yes` was not given, the command prints the plan and exits 1
without applying it. A cron job that would silently rename an analyst's views because nobody was
there to answer is the failure mode this whole design is built to avoid.

Rename attribution (`~`) comes from the view comments. If a comment is missing — a
hand-created view, or a destination that does not support comments — that view degrades to a
`+`/`-` pair in the display. The apply is unaffected, because a full rebuild does not need to
know what anything used to be called.

### `run-pipeline`

Gains no flags and builds no views. After the catalogs are written it compares the view names
the catalog implies against the views that exist, and reports:

```
Label drift: 2 objects renamed since views were built
  object_3   Courses  ->  Classes
  object_7   Invoice Line  ->  Line Item
Run `knack-elt refresh-views` to update the view layer.
```

Rename attribution here comes from the view comments, exactly as it does in the plan; without
them the report degrades to naming the views that would be added and removed. Read-only, no
prompt, and it does not change the exit code — a drift report is information, not a failure. Silent when there is no drift, and silent when the `_labels` schema does not exist,
so a user who never opts into views never hears about them.

## Error handling

View generation runs after the load has already committed. It cannot corrupt history: it only
ever issues `CREATE SCHEMA`, `CREATE VIEW`, and `DROP VIEW` against a schema that holds nothing
but generated views.

- `refresh-views` exits 1 if any view fails, after attempting the rest, and lists each failure
  with the object it belongs to. The transaction means a failed apply leaves the previous view
  set in place rather than a half-built one.
- The drift check inside `run-pipeline` is wrapped so it can never fail the command. A
  successful load that cannot be described is still a successful load.

## Testing

Offline, in `tests/test_pipeline_offline.py`, against synthetic `Application` fixtures and real
DuckDB files — the existing pattern. Every case is a shape a fresh Knack app can legally have.

- A renamed object drops the stale view and creates the new one; the physical table and its
  SCD2 history are byte-identical afterward
- Two objects sharing a label both get `__object_N` suffixes; neither keeps the plain name
- A field labelled `record_id` is suffixed and does not displace the merge key column
- A field labelled `valid_to` gets the same suffixed name in both the live and history views
- Non-Latin object and field labels round-trip through view and column names
- A label containing a double quote produces a working view
- An empty label falls back to the object key
- A record deleted in Knack is absent from the live view, present in `_history`, and carries
  `is_live_in_knack = false`
- A catalog field with no physical column is omitted from the view and counted as skipped
- An object with no physical table is skipped, and the remaining views still build
- Applying twice with no Knack change is a no-op producing an identical view set
- A view rename that swaps two names between objects applies correctly (the ordering hazard)
- `run-pipeline` on a warehouse with no `_labels` schema reports no drift and creates nothing

## Open risk

`COMMENT ON VIEW` and `duckdb_views().comment` are verified against local DuckDB. They are
**not** verified against MotherDuck, which has never been executed in this project at all. The
degradation is graceful by construction — missing comments cost rename attribution in the plan
display and nothing else — but the first MotherDuck run should confirm it rather than assume.
