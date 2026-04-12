# dynamo-io

[![PyPI version](https://badge.fury.io/py/dynamo-io.svg)](https://pypi.org/project/dynamo-io/)
[![CI](https://github.com/rocketboosters/dynamo-io/actions/workflows/ci.yml/badge.svg)](https://github.com/rocketboosters/dynamo-io/actions/workflows/ci.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Code style: flake8](https://img.shields.io/badge/code%20style-flake8-white)](https://github.com/PyCQA/flake8)
[![Code style: mypy](https://img.shields.io/badge/code%20style-mypy-white)](http://mypy-lang.org/)
[![PyPI - License](https://img.shields.io/pypi/l/dynamo-io)](https://pypi.org/project/dynamo-io/)

`dynamo-io` is an opinionated Python library for working with DynamoDB single-table designs. It provides:

- Dataclass-based record models
- Schema definitions for primary keys, secondary index keys, and typed attributes
- Helpers for inserts, upserts, deletes, point reads, partition queries, and indexed queries
- Response wrappers for raw rows and deserialized records
- An in-memory mock DynamoDB client and table helpers for unit and scenario tests

The package targets Python `>=3.11,<4.0` and works with the low-level `boto3` DynamoDB client API.

## Installation

Using `pip`:

```bash
pip install dynamo-io
```

Using Poetry:

```bash
poetry add dynamo-io
```

## What This Library Assumes

This library is built around a normalized single-table shape:

- Primary partition key lives in `pk`
- Primary sort key lives in `sk`
- Additional indexed key columns may live in `g1k`, `g2k`, and `g3k`
- Common metadata columns `created_at`, `updated_at`, and `expires_at` are included on every `Record`

It expects a low-level DynamoDB client, for example:

```python
import boto3

client = boto3.client("dynamodb")
```

Do not pass a DynamoDB resource/table object. The functions call client methods such as `get_item`, `update_item`, `batch_write_item`, and `get_paginator`.

## Quick Start

```python
import dataclasses
import datetime

import boto3
import dynamo_io as dio


@dataclasses.dataclass(frozen=True)
class Product(dio.Record):
    product_id: dio.TypeHints.KeyColumn = None
    sku: dio.TypeHints.KeyColumn = None
    category_key: dio.TypeHints.String = None
    name: dio.TypeHints.String = None
    inventory: dio.TypeHints.Integer = None
    launched_at: dio.TypeHints.Datetime = None

    schema: dio.SchemaType = dio.Schema(
        partition_key=dio.PartitionColumn("product_id", "product:"),
        sort_key=dio.SortColumn("sku", "sku:"),
        columns=(
            dio.GlobalFirstColumn("category_key", "category:"),
            dio.StringColumn("name"),
            dio.IntegerColumn("inventory"),
            dio.DatetimeColumn("launched_at"),
        ),
    )


client = boto3.client("dynamodb")
table_name = "catalog"

record = Product(
    product_id="product:123",
    sku="sku:red-small",
    category_key="category:shirts",
    name="Red Small Shirt",
    inventory=10,
    launched_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
)

dio.upsert(client, table_name, record)

result = dio.get_record(
    client=client,
    table_name=table_name,
    source=Product(product_id="product:123", sku="sku:red-small"),
)

loaded = result.record
```

## Defining Records

Create a dataclass that subclasses `dio.Record` and assign a `schema` class variable.

```python
@dataclasses.dataclass(frozen=True)
class ExampleRecord(dio.Record):
    account_id: dio.TypeHints.KeyColumn = None
    object_id: dio.TypeHints.KeyColumn = None
    status: dio.TypeHints.String = None
    enabled: dio.TypeHints.Boolean = None

    schema: dio.SchemaType = dio.Schema(
        partition_key=dio.PartitionColumn("account_id", "account:"),
        sort_key=dio.SortColumn("object_id", "object:"),
        columns=(
            dio.StringColumn("status"),
            dio.BooleanColumn("enabled"),
        ),
    )
```

### Record Conventions

- `schema` must be a class variable with type hint `dio.SchemaType`
- The base `Record` class always contributes `created_at`, `updated_at`, and `expires_at`
- A record can omit the sort key by setting `sort_key=None` in the schema
- Field values are serialized directly; this library does not prepend key prefixes for you
- In practice, key values should already be in the exact DynamoDB form you want stored, such as `"product:123"` or `"sku:red-small"`

### Schema Components

- `PartitionColumn(name, value_prefix)` defines the field that maps to table key `pk`
- `SortColumn(name, value_prefix)` defines the field that maps to table key `sk`
- `GlobalFirstColumn(name, value_prefix)` maps to `g1k`
- `GlobalSecondColumn(name, value_prefix)` maps to `g2k`
- `GlobalThirdColumn(name, value_prefix)` maps to `g3k`
- `Column(name, data_type)` is the generic typed field definition
- `StringColumn`, `IntegerColumn`, `DatetimeColumn`, etc. are typed conveniences
- `MapColumn(name, children=...)` describes nested map attributes
- Any column can use `key="actual_dynamo_attribute_name"` to store under a different attribute name
- Any column can use `computed=True` to write the field but exclude it when materializing records from rows

### Supported Type Hints and Column Types

Available `TypeHints` aliases:

- `TypeHints.Boolean`
- `TypeHints.BinarySet`
- `TypeHints.String`
- `TypeHints.Bytes`
- `TypeHints.Date`
- `TypeHints.Datetime`
- `TypeHints.Float`
- `TypeHints.Integer`
- `TypeHints.Index`
- `TypeHints.StringSet`
- `TypeHints.FloatSet`
- `TypeHints.IntegerSet`
- `TypeHints.Timestamp`
- `TypeHints.List`
- `TypeHints.Map`
- `TypeHints.KeyColumn`

Available typed column helpers:

- `BooleanColumn`
- `BinarySetColumn`
- `StringColumn`
- `BytesColumn`
- `DateColumn`
- `DatetimeColumn`
- `FloatColumn`
- `FloatSetColumn`
- `IntegerColumn`
- `IntegerSetColumn`
- `ListColumn`
- `MapColumn`
- `StringSetColumn`
- `TimestampColumn`

## Writing Data

### `upsert`

`upsert(client, table_name, record)` writes a single record with `UpdateItem`.

- Uses the record's primary key as `Key`
- Builds `ExpressionAttributeNames`, `ExpressionAttributeValues`, and `UpdateExpression`
- Returns a `SingleRecordResponse`
- Uses `if_not_exists` for `created_at`
- Always writes the latest `updated_at` if present on the record

```python
result = dio.upsert(client, "catalog", record)
updated_record = result.record
```

### `insert_records`

`insert_records(client, table_name, records)` performs a batch write of `PutRequest` items.

- Accepts any iterable of `Record` instances
- Retries unprocessed items with exponential backoff for up to 10 attempts
- Raises `RuntimeError` if items still remain unprocessed
- Returns a basic `Response`

```python
dio.insert_records(client, "catalog", [record_a, record_b, record_c])
```

Use this when you want batch insert behavior rather than attribute-level updates.

### `transacts`

`transacts(client, table_name, puts=None, updates=None, deletes=None)` builds a single `transact_write_items` request.

```python
dio.transacts(
    client=client,
    table_name="catalog",
    puts=[
        Product(product_id="product:1", sku="sku:a"),
    ],
    updates=[
        Product(product_id="product:2", sku="sku:b", inventory=20),
    ],
    deletes=[
        Product(product_id="product:3", sku="sku:c"),
    ],
)
```

### `remove`

`remove(client, table_name, record)` deletes one record by its primary key.

```python
dio.remove(
    client=client,
    table_name="catalog",
    record=Product(product_id="product:123", sku="sku:red-small"),
)
```

### Removing Attributes During Upsert

Use the sentinel `dio.DELETE` to remove an attribute from an existing item during `upsert` or transactional update generation.

```python
record = Product(
    product_id="product:123",
    sku="sku:red-small",
    name=dio.DELETE,
)

dio.upsert(client, "catalog", record)
```

Empty strings are also treated as removals in update expressions. `None` means "leave unchanged" for update generation and is omitted from serialized writes.

## Reading Data

### Raw Row Helpers

These helpers return DynamoDB rows in low-level attribute-value format.

#### `get_row`

Read one row by primary key.

```python
result = dio.get_row(
    client=client,
    table_name="catalog",
    partition_key_value="product:123",
    sort_key_value="sku:red-small",
)

row = result.row
```

#### `get_rows_for_partition`

Query a partition, optionally filtering by sort key prefix or range bounds.

```python
result = dio.get_rows_for_partition(
    client=client,
    table_name="catalog",
    partition_key_value="category:shirts",
    sort_key_starts="product:",
    index=dio.Indexes.G1_PARTITION,
    limit=100,
)
```

Parameters:

- `sort_key_starts`: builds a `begins_with` condition on the queried index sort key
- `before_sort_key`: adds `<`
- `after_sort_key`: adds `>`
- `index`: defaults to `Indexes.STANDARD`
- `limit`: optional DynamoDB query limit

#### `get_indexed_row` and `get_indexed_rows`

Query an index directly by explicit partition/sort values:

```python
result = dio.get_indexed_row(
    client=client,
    table_name="catalog",
    partition_key_value="category:shirts",
    sort_key_value="product:123",
    index=dio.Indexes.G1_PARTITION,
)
```

### Record Helpers

These helpers deserialize matching rows back into `Record` instances.

#### `get_record`

Fetch one record by primary key using a partially populated source record.

```python
result = dio.get_record(
    client=client,
    table_name="catalog",
    source=Product(product_id="product:123", sku="sku:red-small"),
)

record = result.record
```

#### `get_records_for_partition`

Query multiple rows and map them to known record classes.

```python
result = dio.get_records_for_partition(
    client=client,
    table_name="catalog",
    partition_key_value="category:shirts",
    index=dio.Indexes.G1_PARTITION,
    record_classes=[Product],
)

records = result.records
```

Notes:

- Only rows whose schema matches one of the provided `record_classes` are returned as records
- Rows with unknown fields or mismatched key prefixes are skipped
- The raw rows remain available on `result.rows`

#### `get_indexed_record` and `get_indexed_records`

Query an index using a source record to supply the relevant index key values.

```python
result = dio.get_indexed_record(
    client=client,
    table_name="catalog",
    source=Product(
        category_key="category:shirts",
        product_id="product:123",
    ),
    index=dio.Indexes.G1_PARTITION,
)
```

For source-based indexed queries, the library inspects the source record's schema to find fields that correspond to the requested index's partition and sort key attributes.

### `read_entire_table`

`read_entire_table(client, table_name, max_page_count=100)` performs a scan across the whole table.

```python
result = dio.read_entire_table(client, "catalog", max_page_count=25)
```

This is explicitly a debugging helper. It returns a `ScannedRowResponse` with:

- `rows`
- `pages`
- `completed`

If the scan exceeds `max_page_count`, `completed` is `False`.

## Indexes

The package exposes predeclared `Indexes` values that describe common key layouts:

- `Indexes.STANDARD`
- `Indexes.INVERTED`
- `Indexes.PARTITION_G1`
- `Indexes.PARTITION_G2`
- `Indexes.PARTITION_G3`
- `Indexes.SORT_G1`
- `Indexes.SORT_G2`
- `Indexes.SORT_G3`
- `Indexes.G1_PARTITION`
- `Indexes.G1_SORT`
- `Indexes.G1_G2`
- `Indexes.G1_G3`
- `Indexes.G2_PARTITION`
- `Indexes.G2_SORT`
- `Indexes.G2_G1`
- `Indexes.G2_G3`
- `Indexes.G3_PARTITION`
- `Indexes.G3_SORT`
- `Indexes.G3_G1`
- `Indexes.G3_G2`

Each `Index` definition carries:

- `id`
- `name`
- `partition_key`
- `sort_key`

These objects are used by query helpers to build `KeyConditionExpression` and optional `IndexName` values.

## Serialization Behavior

The library serializes record values to DynamoDB's low-level attribute-value format.

Examples:

- strings become `{"S": "..."}`
- integers become `{"N": "..."}`
- booleans become `{"BOOL": ...}`
- datetimes become UTC ISO strings like `2021-02-03T00:00:00Z`
- timestamps become epoch seconds stored as `{"N": "..."}`
- sets become homogeneous DynamoDB set types (`SS`, `NS`, `BS`)
- maps become nested `{"M": ...}` values

Deserialization reverses that process when building records from rows.

## Response Objects

Read and write helpers return small dataclasses instead of bare dictionaries.

### Base response types

- `Response`: raw `request` and `response`
- `SingleRowResponse`: adds `row`
- `PagedRowResponse`: adds `pages`, `rows`, `first_row`, `iter_rows()`
- `ScannedRowResponse`: adds `completed`

### Record response types

- `SingleRecordResponse`: adds `record`
- `PagedRecordResponse`: adds `records`, `first_record`, `iter_records()`
- `ScannedRecordResponse`: defined for scanned record use cases

All response types expose `to_debug_dict()` for compact debug logging.

## In-Memory Mocking for Tests

The package includes `dynamo_io.mock`, which provides an in-memory client compatible with this library's read/write helpers.

```python
import dynamo_io as dio
from dynamo_io import mock

client = mock.MockDynamoClient()

dio.upsert(
    client=client,
    table_name="NA",
    record=Product(
        product_id="product:123",
        sku="sku:red-small",
        category_key="category:shirts",
        inventory=10,
    ),
)

result = dio.get_record(
    client=client,
    table_name="NA",
    source=Product(product_id="product:123", sku="sku:red-small"),
)
```

### Mock Formatting Helpers

`dynamo_io.mock` exports helpers for building raw DynamoDB attribute values:

- `mock.string(value)`
- `mock.integer(value)`
- `mock.number(value)`
- `mock.boolean(value)`
- `mock.timestamp(...)`
- `mock.date_time(...)`

Example:

```python
from dynamo_io import mock as m

client = m.MockDynamoClient()
client.table.add_rows(
    {"pk": m.string("foo"), "sk": m.string("bar1"), "g1k": m.string("a")},
    {"pk": m.string("foo"), "sk": m.string("bar2"), "g1k": m.string("a")},
)
```

### Mock Table Helpers

`client.table` exposes a `MockTable` with helpers that are useful in unit and scenario tests:

- `add_row`, `add_rows`
- `add_record`, `add_records`
- `update_row`, `update_rows`
- `update_record`, `update_records`
- `delete_key`, `delete_row`, `delete_rows`
- `delete_record`, `delete_records`
- `all_records(record_class)`
- `find_rows(needles)`
- `find_records(needles, record_class)`
- `assert_has_key(key)`
- `assert_row_values(key, comparisons)`
- `assert_matching_row_values(partition_key_value, sort_key_starts, comparisons, ...)`

String lookups in `find_rows` and `find_records` support wildcard matching via shell-style patterns such as `"prod:s*"`.

### Mock Comparison Utilities

The mock package also exports comparison helpers used in assertions:

- `is_anything`
- `is_in`
- `is_instance_of`
- `is_iso`
- `is_like`
- `is_match`
- `is_not_null`
- `is_optional`

## Practical Example

```python
import dataclasses

import dynamo_io as dio
from dynamo_io import mock


@dataclasses.dataclass(frozen=True)
class Product(dio.Record):
    product: dio.TypeHints.KeyColumn = None
    line: dio.TypeHints.KeyColumn = None
    category: dio.TypeHints.String = None
    units: dio.TypeHints.Integer = None

    schema: dio.SchemaType = dio.Schema(
        partition_key=dio.PartitionColumn("product", "prod:"),
        sort_key=dio.SortColumn("line", "line:"),
        columns=(
            dio.GlobalFirstColumn("category", "cat:"),
            dio.IntegerColumn("units"),
        ),
    )


client = mock.MockDynamoClient()

dio.insert_records(
    client=client,
    table_name="NA",
    records=[
        Product(
            product="prod:twix",
            line="line:candy-bar",
            category="cat:candy",
            units=10,
        ),
        Product(
            product="prod:skittles",
            line="line:candy-pieces",
            category="cat:candy",
            units=5,
        ),
    ],
)

single = dio.get_record(
    client=client,
    table_name="NA",
    source=Product(product="prod:twix", line="line:candy-bar"),
)

by_category = dio.get_records_for_partition(
    client=client,
    table_name="NA",
    partition_key_value="cat:candy",
    index=dio.Indexes.G1_PARTITION,
    record_classes=[Product],
)
```

## Development

This repository is managed with Poetry and Taskipy.

Install dependencies:

```bash
poetry install
```

Run tests:

```bash
poetry run task test
```

Run the full local check suite:

```bash
poetry run task check
```

Available tasks in `pyproject.toml` include:

- `task black`
- `task flake8`
- `task mypy`
- `task radon`
- `task test`
- `task lint`
- `task check`

## Public API Summary

Top-level exports include:

- Record and schema types: `Record`, `Schema`, `SchemaType`, `Column`, typed column helpers, `PartitionColumn`, `SortColumn`, `GlobalFirstColumn`, `GlobalSecondColumn`, `GlobalThirdColumn`
- Type helpers: `DynamoType`, `DynamoTypes`, `TypeHints`, `DELETE`
- Index helpers: `Index`, `Indexes`
- Read functions: `get_row`, `get_record`, `get_rows_for_partition`, `get_records_for_partition`, `get_indexed_row`, `get_indexed_rows`, `get_indexed_record`, `get_indexed_records`, `read_entire_table`
- Write functions: `insert_records`, `upsert`, `remove`, `transacts`
- Response types: `Response`, `SingleRowResponse`, `PagedRowResponse`, `SingleRecordResponse`, `PagedRecordResponse`

## License

MIT
