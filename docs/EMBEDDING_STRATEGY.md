# Embedding Strategy for Knack Data in LanceDB

## The Challenge

Knack records are structured documents with many typed fields (text, numbers, dates, connections, etc.). How do we handle this in LanceDB?

## The Solution: Hybrid Storage + Selective Embedding

**Key Principle:** Store all fields as columns, embed only relevant text fields for semantic search.

## LanceDB Record Structure

```python
# Example: Customer record in LanceDB
{
    # === Original Knack Fields (preserved as-is) ===
    "id": "rec_123",
    "field_23": "John Doe",                              # short_text (name)
    "field_24": "john@acme.com",                         # email
    "field_25": "object_8_rec_456",                      # connection (company)
    "field_26": "2024-01-15T10:30:00Z",                  # date_time (created)
    "field_27": "Premium tier customer. Requested custom integration for inventory management. Follow up in Q2.", # paragraph_text (notes)
    "field_28": 150000,                                  # number (revenue)
    "field_29": "Active",                                # multiple_choice (status)

    # === Generated Fields ===
    "content": "John Doe john@acme.com Premium tier customer. Requested custom integration...",  # Concatenated embeddable text
    "vector": [0.123, -0.456, 0.789, ...],              # 384-dim embedding (all-MiniLM-L6-v2)

    # === Metadata ===
    "_object_key": "object_1",
    "_object_name": "Customers",
    "_synced_at": "2024-01-20T08:00:00Z",
}
```

## Field Selection Logic

### Fields to Embed (Text Content)

Embed fields that contain human-readable text useful for semantic search:

| Knack Field Type | Embed? | Reason | Example |
|------------------|--------|--------|---------|
| `short_text` | ✅ YES | Names, titles, short descriptions | "John Doe", "Project Alpha" |
| `paragraph_text` | ✅ YES | Rich content for semantic search | "Customer requested custom integration..." |
| `rich_text` | ✅ YES (strip HTML) | Long-form content | "<p>Meeting notes...</p>" |
| `email` | ⚠️ MAYBE | Sometimes useful for search | "john@acme.com" |
| `name` | ✅ YES | Composite name fields | "First: John, Last: Doe" |
| `address` | ✅ YES | Geographic text | "123 Main St, San Francisco, CA" |
| `multiple_choice` | ⚠️ MAYBE | If values are descriptive | "Premium Tier", "Active" |

### Fields NOT to Embed (Keep as Columns)

Store but don't embed - use SQL queries instead:

| Knack Field Type | Embed? | Reason | Query With |
|------------------|--------|--------|------------|
| `number` | ❌ NO | Numeric filtering, not semantic | SQL: `WHERE revenue > 100000` |
| `currency` | ❌ NO | Numeric value | SQL: `WHERE amount > 1000` |
| `date_time` | ❌ NO | Temporal filtering | SQL: `WHERE created > '2024-01-01'` |
| `boolean` | ❌ NO | Binary value | SQL: `WHERE is_active = true` |
| `connection` | ❌ NO | Just IDs (meaningless text) | SQL joins or follow relationships |
| `auto_increment` | ❌ NO | Just a number | SQL: `WHERE order_number = 12345` |
| `rating` | ❌ NO | Numeric value | SQL: `WHERE rating >= 4` |
| `timer` | ❌ NO | Numeric duration | SQL: `WHERE duration_seconds > 3600` |

### Special Cases

| Field Type | Strategy |
|------------|----------|
| `equation` | If result is text, embed it. If numeric, don't. |
| `concatenation` | Usually YES - often combines text fields |
| `count` | NO - it's just a number |
| `sum/average/min/max` | NO - numeric aggregations |
| `link` | MAYBE - embed the URL or linked text if relevant |
| `file/image` | NO - store URL/path only (could extract text with OCR/AI in future) |

## Implementation: Field Type Mapping

```python
from knack_sleuth import load_app_metadata

# Field types that should be embedded
EMBEDDABLE_TYPES = {
    'short_text',
    'paragraph_text',
    'rich_text',
    'email',
    'name',
    'address',
    'phone',
    'link',          # The URL text itself can be useful
    'multiple_choice',  # If values are descriptive
    'concatenation',    # Usually combines text fields
}

# Field types to exclude from embedding
NON_EMBEDDABLE_TYPES = {
    'number',
    'currency',
    'date_time',
    'boolean',
    'connection',
    'auto_increment',
    'rating',
    'timer',
    'count',
    'sum',
    'average',
    'min',
    'max',
    'equation',  # Could be text or number - handle specially
    'image',
    'file',
}

def should_embed_field(field: KnackField) -> bool:
    """Determine if a field should be included in embedding"""

    # Basic type check
    if field.type in EMBEDDABLE_TYPES:
        return True

    if field.type in NON_EMBEDDABLE_TYPES:
        return False

    # Special case: equations can be text or numeric
    if field.type == 'equation':
        # Check if equation format suggests text output
        # This is heuristic - might need refinement
        equation_format = field.format
        if equation_format in ('text', 'concatenate'):
            return True
        return False

    # Default: don't embed unknown types
    return False

def extract_embeddable_content(record: dict, obj: KnackObject) -> str:
    """Extract and concatenate embeddable fields from a record"""

    parts = []

    for field in obj.fields:
        if not should_embed_field(field):
            continue

        value = record.get(field.key)
        if not value:
            continue

        # Handle different field type formats
        if field.type == 'rich_text':
            # Strip HTML tags
            import re
            value = re.sub(r'<[^>]+>', '', value)

        elif field.type == 'name':
            # Name fields are often objects: {"first": "John", "last": "Doe"}
            if isinstance(value, dict):
                value = f"{value.get('first', '')} {value.get('last', '')}".strip()

        elif field.type == 'address':
            # Address fields are objects: {"street": "...", "city": "...", ...}
            if isinstance(value, dict):
                addr_parts = [
                    value.get('street', ''),
                    value.get('street2', ''),
                    value.get('city', ''),
                    value.get('state', ''),
                    value.get('zip', ''),
                ]
                value = ', '.join(p for p in addr_parts if p)

        elif field.type == 'email':
            # Email can be object: {"email": "john@acme.com"}
            if isinstance(value, dict):
                value = value.get('email', '')

        # Convert to string and add to parts
        parts.append(str(value))

    # Join with spaces, clean up
    content = ' '.join(parts)
    content = ' '.join(content.split())  # Normalize whitespace

    return content
```

## Multi-Field vs Single-Field Embedding

### Option 1: Single Concatenated Embedding (Recommended)

**One embedding per record, concatenate all relevant text fields**

```python
content = "John Doe john@acme.com Premium customer, requested custom integration"
embedding = embed(content)  # Single 384-dim vector

# Store in LanceDB
record = {
    "field_23": "John Doe",
    "field_24": "john@acme.com",
    "field_27": "Premium customer...",
    "content": content,           # Concatenated
    "vector": embedding,          # Single vector
}
```

**Pros:**
- ✅ Simple - one vector per record
- ✅ Efficient storage
- ✅ Fast search (single vector similarity)
- ✅ Works well for most use cases

**Cons:**
- ❌ Can't weight individual fields differently
- ❌ Might include irrelevant text in embedding

**Use when:** Default choice for most Knack objects

### Option 2: Multiple Embeddings per Record (Advanced)

**Separate embeddings for different field groups**

⚠️ **Important:** If your records have semantically distinct sections for different audiences (e.g., client info vs performer info in event planning), you should use this approach. See [MULTI_EMBEDDING_EXAMPLE.md](MULTI_EMBEDDING_EXAMPLE.md) for a complete example.

```python
# Example: Support ticket with multiple text areas
record = {
    "id": "rec_456",

    # Original fields
    "field_10": "Login not working",              # title
    "field_11": "Can't access dashboard...",      # description
    "field_12": "Tried clearing cache...",        # customer_notes
    "field_13": "Issue with OAuth token...",      # internal_notes

    # Multiple embeddings
    "title_vector": embed("Login not working"),
    "description_vector": embed("Can't access dashboard..."),
    "customer_vector": embed("Tried clearing cache..."),
    "internal_vector": embed("Issue with OAuth token..."),

    # Or: concatenate related fields
    "public_content": "Login not working Can't access dashboard Tried clearing cache",
    "public_vector": embed(public_content),

    "internal_content": "Issue with OAuth token...",
    "internal_vector": embed(internal_content),
}
```

**Search strategy:**
```python
# Search public-facing content only
results = table.search(query_embedding, vector_column_name="public_vector")

# Or search internal notes only
results = table.search(query_embedding, vector_column_name="internal_vector")

# Or search both and combine scores
public_results = table.search(q, vector_column_name="public_vector")
internal_results = table.search(q, vector_column_name="internal_vector")
combined = merge_and_rerank(public_results, internal_results)
```

**Pros:**
- ✅ Field-level search precision
- ✅ Can separate public/private content
- ✅ Weight different sections differently

**Cons:**
- ❌ More complex implementation
- ❌ More storage (multiple vectors per record)
- ❌ Slower (multiple searches or custom logic)

**Use when:**
- **Different audiences search different sections** (e.g., event planning: clients search wedding info, performers search equipment info)
- Large records with distinct semantic sections (e.g., support tickets: title, description, resolution)
- Need to separate public vs internal content
- Want field-specific search (search titles only, then descriptions)
- Single embedding would dilute search quality by mixing unrelated contexts

### Option 3: Hybrid Multi-Column (Best of Both)

**One main embedding + keep all fields as searchable columns**

```python
record = {
    # All original fields as columns (for SQL queries and display)
    "id": "rec_123",
    "name": "John Doe",
    "email": "john@acme.com",
    "company_id": "object_8_rec_456",
    "created_date": "2024-01-15",
    "notes": "Premium customer, requested custom integration",
    "revenue": 150000,
    "status": "Active",

    # Single concatenated embedding for semantic search
    "content": "John Doe john@acme.com Premium customer, requested custom integration",
    "vector": embed(content),
}

# Query with hybrid approach
results = (
    table.search(embed("high value customers"))  # Vector search
    .where("revenue > 100000")                    # SQL filter
    .where("status = 'Active'")                   # SQL filter
    .limit(10)
    .to_list()
)
```

**This is the recommended approach** because:
- ✅ Simple single embedding for semantic search
- ✅ All fields preserved for exact matching
- ✅ Powerful hybrid queries (semantic + structured)
- ✅ Straightforward to implement

## Complete Example

```python
from knack_sleuth import load_app_metadata
from knack_elt import export_data
import lancedb
from sentence_transformers import SentenceTransformer

# Load schema
app = load_app_metadata(app_id="your_app_id")

# Initialize embedding model
embedder = SentenceTransformer('all-MiniLM-L6-v2')

# Connect to LanceDB
db = lancedb.connect("./lancedb")

# Process each object
for obj in app.objects:
    print(f"Processing {obj.name} ({obj.key})...")

    # Get records from Knack
    records = export_data(obj.key)

    # Transform for LanceDB
    lance_records = []
    for record in records:
        # Extract embeddable content
        content = extract_embeddable_content(record, obj)

        # Generate embedding (only if there's content)
        if content.strip():
            embedding = embedder.encode(content).tolist()
        else:
            # No embeddable content - use zero vector or skip
            embedding = [0.0] * 384

        # Build LanceDB record
        lance_record = {
            # All original Knack fields preserved
            **record,

            # Generated fields for search
            "content": content,
            "vector": embedding,

            # Metadata
            "_object_key": obj.key,
            "_object_name": obj.name,
            "_synced_at": datetime.now().isoformat(),
        }

        lance_records.append(lance_record)

    # Create table
    table_name = f"knack_{obj.key}"
    db.create_table(table_name, lance_records, mode="overwrite")

    print(f"✓ Created table {table_name}")
    print(f"  - {len(lance_records)} records")
    print(f"  - {len([r for r in lance_records if r['content'].strip()])} with embeddable content")
    print(f"  - {len([f for f in obj.fields if should_embed_field(f)])} embeddable fields")
```

## Querying Patterns

### Pattern 1: Pure Semantic Search
```python
# Find records similar to query (uses embedding)
results = table.search(embed("customer complained about billing")).limit(10).to_list()
```

### Pattern 2: Pure SQL Search
```python
# Exact/structured queries (uses columns)
results = table.search().where("status = 'Active' AND revenue > 100000").to_list()
```

### Pattern 3: Hybrid Search (Recommended)
```python
# Semantic discovery + structured filtering
results = (
    table.search(embed("payment issues"))
    .where("created_date > '2024-01-01'")
    .where("status != 'Resolved'")
    .limit(10)
    .to_list()
)
```

### Pattern 4: Field-Specific Text Search (SQL LIKE)
```python
# When you need exact substring matching (not semantic)
results = table.search().where("email LIKE '%@acme.com'").to_list()
```

## Performance Considerations

### Embedding Size

| Model | Dimensions | Speed | Quality | Use Case |
|-------|-----------|-------|---------|----------|
| `all-MiniLM-L6-v2` | 384 | Very Fast | Good | **Recommended** - best balance |
| `all-mpnet-base-v2` | 768 | Medium | Better | Higher quality, slower |
| `all-MiniLM-L12-v2` | 384 | Fast | Better | Good middle ground |
| OpenAI `text-embedding-3-small` | 1536 | API call | Excellent | If using OpenAI API |

### Content Length

```python
# Limit content length to avoid truncation
MAX_CONTENT_LENGTH = 512  # tokens (~2000 chars)

def extract_embeddable_content(record: dict, obj: KnackObject) -> str:
    content = # ... build content ...

    # Truncate if needed (models have token limits)
    if len(content) > 8000:  # ~512 tokens
        content = content[:8000] + "..."

    return content
```

### Batch Processing

```python
# Embed in batches for efficiency
def embed_batch(texts: list[str]) -> list[list[float]]:
    """Embed multiple texts at once (faster than one-by-one)"""
    embeddings = embedder.encode(texts, show_progress_bar=True)
    return embeddings.tolist()

# Use during ingestion
contents = [extract_embeddable_content(r, obj) for r in records]
embeddings = embed_batch(contents)  # Much faster than loop

for record, content, embedding in zip(records, contents, embeddings):
    lance_record = {**record, "content": content, "vector": embedding}
    lance_records.append(lance_record)
```

## Summary

**Recommended Strategy:**

1. ✅ **Store all Knack fields as columns** (preserve structure)
2. ✅ **Embed only text fields** (names, notes, descriptions)
3. ✅ **Single concatenated embedding per record** (simple, effective)
4. ✅ **Use hybrid queries** (semantic search + SQL filters)

This gives you:
- Powerful semantic search on text content
- Fast exact/structured queries on other fields
- Simple architecture that's easy to maintain
- Best of both vector and relational worlds

**When to consider alternatives:**
- Very large text fields → Split into chunks with multiple embeddings
- Distinct content sections → Separate embeddings per section
- Field-specific search → Multiple vector columns
