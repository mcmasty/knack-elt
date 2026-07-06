# Multi-Embedding Example: Event Planning Use Case

## The Problem

In event planning, a single record contains semantically distinct information for different audiences:

**Event Record:**
- **Client/Wedding Info:** Bride, groom, planner, ceremony details, guest count
- **Performance Info:** Band, lighting, sound equipment, technical riders, drivers

**Search Requirements:**
- Wedding planners search: "bride who wants jazz", "outdoor ceremony in June"
- Performers search: "events with outdoor sound", "needs lighting tech"

**Problem with single embedding:** Mixing both domains dilutes semantic search quality.

## Solution: Separate Embeddings per Domain

### LanceDB Record Structure

```python
{
    # === Original Knack Fields ===
    "id": "rec_event_123",
    "event_date": "2024-06-15",
    "venue": "Garden Estate",

    # Client/Wedding fields
    "bride_name": "Sarah Johnson",
    "groom_name": "Mike Chen",
    "planner_name": "Emily Rodriguez",
    "ceremony_notes": "Outdoor ceremony under oak tree, wants jazz during cocktail hour, 150 guests",
    "guest_count": 150,
    "budget": 50000,

    # Performance fields
    "band_name": "Jazz Quartet",
    "sound_requirements": "4 wireless mics, outdoor PA system, soundcheck at 2pm",
    "lighting_needs": "String lights, uplighting for tent, needs tech on-site",
    "equipment_notes": "Large truck access required for sound equipment",
    "performer_count": 8,

    # === Multiple Embeddings (separate vector columns) ===
    "client_content": "Bride Sarah Johnson Groom Mike Chen Planner Emily Rodriguez Outdoor ceremony under oak tree, wants jazz during cocktail hour, 150 guests Garden Estate",
    "client_vector": [0.123, -0.456, ...],  # Embedding of client_content

    "performer_content": "Jazz Quartet 4 wireless mics, outdoor PA system, soundcheck at 2pm String lights, uplighting for tent, needs tech on-site Large truck access required for sound equipment",
    "performer_vector": [0.789, 0.234, ...],  # Embedding of performer_content

    # Metadata
    "_object_key": "object_5",
    "_object_name": "Events",
}
```

## Implementation

### Step 1: Define Semantic Sections

```python
from knack_sleuth import KnackObject

def get_semantic_sections(obj: KnackObject) -> dict[str, list[str]]:
    """
    Define which fields belong to which semantic sections.

    Returns dict mapping section name -> list of field keys
    """

    # This could be:
    # 1. Hardcoded per object
    # 2. Configured via YAML
    # 3. Inferred from field names/metadata

    sections = {}

    if obj.key == "object_5":  # Events object
        sections["client"] = [
            "field_10",  # bride_name
            "field_11",  # groom_name
            "field_12",  # planner_name
            "field_13",  # ceremony_notes
            "field_14",  # venue
            "field_15",  # guest_notes
        ]

        sections["performer"] = [
            "field_20",  # band_name
            "field_21",  # sound_requirements
            "field_22",  # lighting_needs
            "field_23",  # equipment_notes
            "field_24",  # technical_rider
        ]

    return sections

def extract_section_content(record: dict, field_keys: list[str]) -> str:
    """Extract and concatenate content from specific fields"""
    parts = []
    for key in field_keys:
        value = record.get(key)
        if value:
            parts.append(str(value))
    return " ".join(parts)
```

### Step 2: Create Multiple Embeddings

```python
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer('all-MiniLM-L6-v2')

def create_lance_record_multi_embedding(record: dict, obj: KnackObject):
    """Create LanceDB record with multiple embeddings"""

    sections = get_semantic_sections(obj)

    # Build base record with all original fields
    lance_record = {**record}

    # Create embedding for each section
    for section_name, field_keys in sections.items():
        # Extract content for this section
        content = extract_section_content(record, field_keys)

        # Generate embedding
        if content.strip():
            embedding = embedder.encode(content).tolist()
        else:
            embedding = [0.0] * 384  # Zero vector if no content

        # Add to record
        lance_record[f"{section_name}_content"] = content
        lance_record[f"{section_name}_vector"] = embedding

    # Add metadata
    lance_record["_object_key"] = obj.key
    lance_record["_object_name"] = obj.name

    return lance_record
```

### Step 3: Full Ingestion Pipeline

```python
from knack_sleuth import load_app_metadata
from knack_elt import export_data
import lancedb

# Load schema
app = load_app_metadata(app_id="your_app_id")

# Initialize
embedder = SentenceTransformer('all-MiniLM-L6-v2')
db = lancedb.connect("./lancedb")

# Process Events object
events_obj = next(o for o in app.objects if o.key == "object_5")
records = export_data(events_obj.key)

# Transform records
lance_records = []
for record in records:
    lance_record = create_lance_record_multi_embedding(record, events_obj)
    lance_records.append(lance_record)

# Create table
db.create_table("knack_object_5", lance_records, mode="overwrite")
```

## Querying with Role-Based Search

### MCP Tool: Role-Aware Search

```python
@mcp.tool()
async def search_events(query: str, role: str, limit: int = 5) -> dict:
    """
    Search events with role-based filtering

    Args:
        query: Semantic search query
        role: "client" or "performer" (determines which embedding to search)
        limit: Number of results
    """

    table = db.open_table("knack_object_5")

    # Generate embedding for query
    query_embedding = embedder.encode(query).tolist()

    # Search appropriate vector column based on role
    if role == "client":
        vector_column = "client_vector"
    elif role == "performer":
        vector_column = "performer_vector"
    else:
        raise ValueError(f"Unknown role: {role}")

    # Vector search on specific column
    matches = (
        table.search(query_embedding, vector_column_name=vector_column)
        .limit(limit)
        .to_list()
    )

    # Optionally fetch fresh data from Knack API
    fresh_records = []
    for match in matches:
        fresh_record = await knack_client.get(f"/objects/object_5/records/{match['id']}")
        fresh_record['_similarity_score'] = 1 - match.get('_distance', 0)
        fresh_records.append(fresh_record)

    return {
        "query": query,
        "role": role,
        "records": fresh_records,
        "searched_section": role,
    }
```

### Usage Examples

```python
# Wedding planner searches
result = await search_events(
    query="bride who wants outdoor jazz ceremony",
    role="client"
)
# Searches ONLY client_vector, ignores performer info

# Band searches
result = await search_events(
    query="events with outdoor sound requirements",
    role="performer"
)
# Searches ONLY performer_vector, ignores client info
```

## Advanced: Multi-Section Search

Sometimes you want to search across multiple sections:

```python
@mcp.tool()
async def search_events_multi(query: str, sections: list[str] = None, limit: int = 10) -> dict:
    """
    Search across multiple sections and combine results

    Args:
        query: Search query
        sections: List of sections to search (e.g., ["client", "performer"])
                 If None, searches all sections
        limit: Total results to return
    """

    table = db.open_table("knack_object_5")
    query_embedding = embedder.encode(query).tolist()

    if sections is None:
        sections = ["client", "performer"]

    all_results = []

    # Search each section
    for section in sections:
        vector_column = f"{section}_vector"

        matches = (
            table.search(query_embedding, vector_column_name=vector_column)
            .limit(limit)
            .to_list()
        )

        # Add section metadata
        for match in matches:
            match['_matched_section'] = section
            all_results.append(match)

    # Combine and re-rank by distance
    all_results.sort(key=lambda x: x.get('_distance', float('inf')))
    top_results = all_results[:limit]

    return {
        "query": query,
        "sections_searched": sections,
        "records": top_results,
    }
```

### Example: Cross-Section Query

```python
# Search both client and performer sections
result = await search_events_multi(
    query="jazz music outdoor venue",
    sections=["client", "performer"]
)
# Might match:
# - Client section: "wants jazz during cocktail hour"
# - Performer section: "Jazz Quartet outdoor PA system"
```

## Configuration via YAML

For maintainability, define sections in config:

```yaml
# .knack/embedding_sections.yaml

objects:
  object_5:  # Events
    sections:
      client:
        fields:
          - field_10  # bride_name
          - field_11  # groom_name
          - field_12  # planner_name
          - field_13  # ceremony_notes
          - field_14  # venue

      performer:
        fields:
          - field_20  # band_name
          - field_21  # sound_requirements
          - field_22  # lighting_needs
          - field_23  # equipment_notes

  object_8:  # Support Tickets
    sections:
      customer_facing:
        fields:
          - field_30  # title
          - field_31  # description
          - field_32  # customer_notes

      internal:
        fields:
          - field_40  # internal_notes
          - field_41  # technical_details
          - field_42  # resolution_steps
```

Load and use:

```python
import yaml

def load_section_config(config_path: str = ".knack/embedding_sections.yaml"):
    with open(config_path) as f:
        return yaml.safe_load(f)

config = load_section_config()
sections = config['objects'][obj.key]['sections']
```

## When to Use Multiple Embeddings

| Use Case | Single Embedding | Multiple Embeddings |
|----------|------------------|---------------------|
| Homogeneous content | ✅ Simple, effective | ❌ Overkill |
| Multiple audiences | ⚠️ Mixed results | ✅ Precise per-audience |
| Distinct semantic sections | ❌ Diluted search | ✅ Section-specific search |
| Role-based access | ❌ Can't filter | ✅ Search by role |
| Large records (>1000 words) | ❌ Context lost | ✅ Chunk semantically |

## Performance Considerations

### Storage

```python
# Single embedding
storage_per_record = 384 floats * 4 bytes = 1.5 KB

# Two embeddings
storage_per_record = 768 floats * 4 bytes = 3 KB

# Reasonable trade-off for better search quality
```

### Search Speed

```python
# Single section search - same speed as single embedding
table.search(q, vector_column_name="client_vector")  # Fast

# Multi-section search - 2x slower (searches two vectors)
# But still fast enough for most use cases
```

## Complete Example

```python
# === Ingestion ===

# Event record from Knack
knack_record = {
    "id": "rec_event_123",
    "field_10": "Sarah Johnson",          # bride_name
    "field_11": "Mike Chen",              # groom_name
    "field_13": "Outdoor ceremony, wants jazz during cocktails",
    "field_20": "Jazz Quartet",           # band_name
    "field_21": "4 wireless mics, outdoor PA, soundcheck 2pm",
}

# Transform for LanceDB
lance_record = {
    **knack_record,

    # Client section
    "client_content": "Sarah Johnson Mike Chen Outdoor ceremony, wants jazz during cocktails",
    "client_vector": embed("Sarah Johnson Mike Chen Outdoor ceremony, wants jazz during cocktails"),

    # Performer section
    "performer_content": "Jazz Quartet 4 wireless mics, outdoor PA, soundcheck 2pm",
    "performer_vector": embed("Jazz Quartet 4 wireless mics, outdoor PA, soundcheck 2pm"),
}

# === Querying ===

# Wedding planner search
results = table.search(
    embed("outdoor jazz ceremony"),
    vector_column_name="client_vector"
).limit(5).to_list()
# Matches client_content, not diluted by performer info

# Band search
results = table.search(
    embed("outdoor sound setup"),
    vector_column_name="performer_vector"
).limit(5).to_list()
# Matches performer_content, not diluted by client info
```

## Summary

For your event planning use case with distinct semantic sections:

1. **Create separate embeddings** for client and performer content
2. **Search the relevant section** based on user role/intent
3. **Store configuration** in YAML for maintainability
4. **Fall back to multi-section search** when needed

This gives you:
- ✅ Precise semantic search per audience
- ✅ No cross-contamination between domains
- ✅ Role-based filtering
- ✅ Flexibility to search across sections when needed

The storage/performance overhead is minimal compared to the search quality improvement.
