# Data Freshness Strategy for Knack RAG System

## The Staleness Problem

**LanceDB is a read replica** - it will always lag behind the live Knack database. This document outlines when to use each data source and how to manage staleness.

## Decision Matrix: LanceDB vs Knack API

### Use Knack API Directly When:

| Scenario | Reason | Example Query |
|----------|--------|---------------|
| **Single record lookup** | Direct API faster than vector search | "Get customer ID 12345" |
| **Real-time data required** | Cannot tolerate any staleness | "What's the current order status?" |
| **Write operations** | Only Knack can write | "Update customer email" |
| **Financial/compliance** | Audit trail, source of truth | "What's the invoice total?" |
| **Frequently changing** | LanceDB always stale | "Current inventory count" |
| **Record count < 10** | API call overhead minimal | "Show these 3 specific records" |

### Use LanceDB When:

| Scenario | Reason | Example Query |
|----------|--------|---------------|
| **Semantic search** | Not possible via Knack API | "Find tickets about payment problems" |
| **Fuzzy/exploratory** | Don't know exact criteria | "Similar customers to this one" |
| **Cross-object analysis** | Complex joins expensive | "Patterns across orders, customers, products" |
| **Analytics/aggregations** | Reduce load on production DB | "Average order value by region over time" |
| **Large result sets** | Vector search with relevance ranking | "All customers mentioning X in notes" |
| **Historical analysis** | Point-in-time snapshots | "Compare to last month's data" |

## Hybrid Patterns

### Pattern 1: Search & Fetch (Recommended for Most Cases)

**Use LanceDB to FIND, Knack API to FETCH**

```python
@mcp.tool()
async def search_and_fetch(query: str, object_key: str, limit: int = 5) -> dict:
    """
    Semantic search in LanceDB, but return fresh data from Knack API

    Best for: Exploratory queries where you don't know exact criteria
    Staleness: Only affects which records are found, not the data returned
    """
    # Phase 1: Vector search to find relevant records (LanceDB)
    lance_table = db.open_table(f"knack_{object_key}")
    matches = lance_table.search(embed(query)).limit(limit).to_list()

    # Phase 2: Fetch fresh data for those records (Knack API)
    fresh_records = []
    for match in matches:
        record = await knack_api.get_record(object_key, match['id'])
        fresh_records.append({
            **record,
            "_similarity": match['_distance'],
            "_found_via": "semantic_search"
        })

    return {
        "query": query,
        "records": fresh_records,
        "source": "hybrid (search: lancedb, data: knack api)",
        "data_freshness": "real-time"
    }
```

**Advantages:**
- ✅ Best of both worlds: semantic search + fresh data
- ✅ Staleness only affects discovery, not accuracy
- ✅ User always sees current state

**Disadvantages:**
- ❌ Additional API calls (N+1 query problem)
- ❌ Slightly slower (network overhead)

**When to use:** Most chat/RAG queries where you need to find something but want fresh data

### Pattern 2: LanceDB Only (Analytics Mode)

**Use LanceDB exclusively for analytics/reporting**

```python
@mcp.tool()
async def analytics_query(object_key: str, sql: str) -> dict:
    """
    Analytics on LanceDB snapshot (accept staleness for speed)

    Best for: Aggregations, trends, historical analysis
    Staleness: Acceptable - analytics don't need real-time data
    """
    table = db.open_table(f"knack_{object_key}")
    results = table.search().where(sql).to_list()

    metadata = table.metadata
    last_sync = metadata.get('last_updated', 'unknown')

    return {
        "results": results,
        "source": "lancedb",
        "last_updated": last_sync,
        "warning": "Data may be stale - use for trends/analytics only"
    }
```

**When to use:**
- Dashboards/reports (don't need real-time)
- Trend analysis over time
- Exploratory data analysis
- Heavy aggregations that would slow production DB

### Pattern 3: Knack Only (Transactional Mode)

**Bypass LanceDB entirely for transactional operations**

```python
@mcp.tool()
async def get_record(object_key: str, record_id: str) -> dict:
    """
    Direct lookup from Knack API (skip LanceDB)

    Best for: Single record fetch, writes, authoritative queries
    Staleness: None - always fresh
    """
    record = await knack_api.get_record(object_key, record_id)

    return {
        "record": record,
        "source": "knack api",
        "data_freshness": "real-time"
    }

@mcp.tool()
async def update_record(object_key: str, record_id: str, data: dict) -> dict:
    """
    Update record in Knack, then sync to LanceDB

    Ensures: User's changes immediately reflected in both systems
    """
    # Update in Knack (source of truth)
    updated_record = await knack_api.update_record(object_key, record_id, data)

    # Immediately sync to LanceDB (avoid showing stale data to user)
    await sync_record_to_lancedb(object_key, updated_record)

    return {
        "record": updated_record,
        "synced_to_lancedb": True
    }
```

**When to use:**
- CRUD operations
- Known record ID
- Financial/compliance queries
- Need authoritative data

### Pattern 4: Smart Routing (AI-Powered Decision)

**Let the agent decide based on query intent**

```python
@mcp.tool()
async def query_data(query: str, object_key: str) -> dict:
    """
    Intelligent routing based on query type

    The agent provides intent hints to route optimally
    """
    intent = classify_intent(query)  # "search" | "lookup" | "analytics"

    if intent == "search":
        return await search_and_fetch(query, object_key)
    elif intent == "lookup":
        # Extract ID from query, use Knack API directly
        record_id = extract_id(query)
        return await get_record(object_key, record_id)
    elif intent == "analytics":
        return await analytics_query(object_key, build_sql(query))
```

## Sync Strategies

### Strategy 1: Full Refresh (Simple, but Slow)

```bash
# Nightly cron job
0 2 * * * python sync_knack_to_lancedb.py --full-refresh
```

**Pros:**
- ✅ Simple to implement
- ✅ Guaranteed consistency
- ✅ No missed updates

**Cons:**
- ❌ Slow for large datasets
- ❌ Up to 24h staleness
- ❌ Wastes resources re-syncing unchanged data

**When to use:** Small datasets (<10k records), data changes infrequently

### Strategy 2: Incremental Sync (Efficient)

```python
# Every 15 minutes
async def incremental_sync():
    """Sync only records modified since last sync"""
    last_sync_time = get_last_sync_time()

    for obj in app.objects:
        # Knack API: filter by modified_date
        updated_records = await knack_api.get_records(
            object_key=obj.key,
            filters=[{"field": "modified_date", "operator": "is after", "value": last_sync_time}]
        )

        # Update LanceDB
        for record in updated_records:
            await update_lancedb_record(obj.key, record)

    set_last_sync_time(datetime.now())
```

**Pros:**
- ✅ Fast (only syncs changes)
- ✅ Lower latency (15min staleness)
- ✅ Less load on Knack API

**Cons:**
- ❌ Requires modified_date field on all objects
- ❌ Doesn't catch deletions easily
- ❌ More complex

**When to use:** Medium datasets (10k-1M records), frequent updates

### Strategy 3: Webhook-Triggered (Real-Time)

```python
# Knack webhook endpoint
@app.post("/webhook/knack/record_update")
async def knack_webhook(event: KnackWebhookEvent):
    """Update LanceDB immediately when Knack record changes"""

    if event.type == "record.created" or event.type == "record.updated":
        await update_lancedb_record(event.object_key, event.record)

    elif event.type == "record.deleted":
        await delete_lancedb_record(event.object_key, event.record_id)

    return {"status": "synced"}
```

**Pros:**
- ✅ Near real-time (seconds of latency)
- ✅ Minimal load (only syncs when needed)
- ✅ Catches all changes including deletes

**Cons:**
- ❌ Requires Knack webhook support
- ❌ Need webhook endpoint infrastructure
- ❌ Potential for missed events (network failures)

**When to use:** Need near real-time search, have webhook infrastructure

### Strategy 4: On-Demand Refresh (User-Triggered)

```python
@mcp.tool()
async def refresh_record(object_key: str, record_id: str) -> dict:
    """Refresh a specific record in LanceDB from Knack"""

    # Fetch fresh data from Knack
    fresh_record = await knack_api.get_record(object_key, record_id)

    # Update LanceDB
    await update_lancedb_record(object_key, fresh_record)

    return {
        "record": fresh_record,
        "refreshed_at": datetime.now().isoformat()
    }
```

**When to use:** User suspects data is stale, critical record they're working with

## Recommended Architecture

### Tier 1: Real-Time Layer (Knack API)
- **Use for:** Lookups by ID, writes, transactional queries
- **Latency:** <500ms
- **Staleness:** None

### Tier 2: Near Real-Time Layer (LanceDB with Webhooks)
- **Use for:** Semantic search, analytics
- **Latency:** 1-5s (if using webhooks)
- **Staleness:** Seconds to minutes
- **Sync:** Webhook-triggered + 15min incremental backup

### Tier 3: Batch Layer (LanceDB with Nightly Refresh)
- **Use for:** Historical analysis, large aggregations
- **Latency:** Varies
- **Staleness:** Up to 24h
- **Sync:** Nightly full refresh

## Implementation Recommendations

### For Most Use Cases (Start Here)

```python
# Default MCP tool set
tools = [
    # Hybrid: Search in LanceDB, fetch from Knack
    "search_and_fetch",       # Most common - combines both

    # Knack only: Single record operations
    "get_record",             # Lookup by ID
    "update_record",          # Write operations
    "create_record",          # Create new
    "delete_record",          # Delete

    # LanceDB only: Analytics (staleness OK)
    "analytics_query",        # Aggregations, trends

    # Utility
    "refresh_record",         # Force sync specific record
]
```

### Sync Schedule

```yaml
# Recommended sync strategy
primary: incremental
  frequency: 15min
  method: modified_date filter

backup: full_refresh
  frequency: daily
  time: 2am

real_time: webhooks (optional)
  enabled: true
  fallback: incremental
```

### User Communication

Always indicate data source and freshness:

```python
# Good: Transparent about staleness
"Found 5 matching customers (via semantic search, showing live data from Knack)"

# Better: Show sync time
"Found 5 matching customers (search index updated 12 minutes ago, showing current data)"

# Best: Actionable if stale
"Found 5 matching customers (search index updated 2 days ago - click here to refresh)"
```

## Staleness Acceptance Table

| Data Type | Max Acceptable Staleness | Recommended Approach |
|-----------|-------------------------|---------------------|
| Financial transactions | 0 (real-time) | Knack API only |
| Customer records | 1 hour | Search & Fetch |
| Product catalog | 1 day | LanceDB + nightly sync |
| Support tickets | 15 min | LanceDB + incremental sync |
| Analytics/reports | 1 week | LanceDB only |
| User profiles | 1 hour | Search & Fetch |
| Inventory counts | 0 (real-time) | Knack API only |
| Historical logs | 1 month | LanceDB only |

## Anti-Patterns to Avoid

❌ **Using LanceDB for authoritative queries**
```python
# BAD: Don't use stale data for critical queries
balance = lancedb.search(f"user_id = '{user_id}'")[0]['balance']
if balance > 100:
    process_refund()  # Could be wrong!
```

✅ **Always fetch fresh data for decisions**
```python
# GOOD: Fetch from Knack for critical operations
balance = await knack_api.get_record('accounts', user_id)['balance']
if balance > 100:
    process_refund()
```

❌ **Showing stale data without disclosure**
```python
# BAD: User doesn't know data might be old
return f"Customer email: {lancedb_record['email']}"
```

✅ **Transparent about source**
```python
# GOOD: Let user know data source
return f"Customer email: {fresh_record['email']} (live data)"
```

❌ **Syncing everything in real-time**
```python
# BAD: Unnecessary load for analytics data
await sync_all_objects_every_second()
```

✅ **Match sync frequency to use case**
```python
# GOOD: Different strategies for different data types
sync_transactions_realtime()  # Financial data
sync_analytics_nightly()      # Reporting data
```

## Monitoring & Alerts

```python
# Track staleness metrics
metrics = {
    "lancedb_lag_seconds": time_since_last_sync,
    "records_out_of_sync": count_of_stale_records,
    "sync_failures_24h": failed_sync_attempts,
}

# Alert if too stale
if metrics["lancedb_lag_seconds"] > 7200:  # 2 hours
    alert("LanceDB sync is lagging - consider investigating")
```

## Summary

**Golden Rule:**
- **Find with LanceDB** (semantic search, fuzzy queries)
- **Fetch from Knack** (authoritative, fresh data)
- **Write to Knack, sync to LanceDB** (maintain consistency)

This hybrid approach leverages the strengths of both systems while minimizing staleness impact.
