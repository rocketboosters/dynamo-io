import typing

from botocore.client import BaseClient

from dynamo_io import definitions
from dynamo_io import recorder


def get_row(
    client: BaseClient,
    table_name: str,
    partition_key_value: str,
    sort_key_value: typing.Optional[str],
) -> definitions.SingleRowResponse:
    """..."""
    key = {"pk": {"S": str(partition_key_value)}}
    if sort_key_value is not None:
        key["sk"] = {"S": str(sort_key_value)}

    request = {"TableName": table_name, "Key": key}

    response = client.get_item(**request)
    return definitions.SingleRowResponse(
        request=request,
        response=response,
        row=response.get("Item"),
    )


def get_record(
    client: BaseClient,
    table_name: str,
    source: "recorder.Record",
) -> recorder.SingleRecordResponse:
    """..."""
    response = get_row(
        client, table_name, source.partition_key_value, source.sort_key_value
    )
    return recorder.SingleRecordResponse(
        request=response.request,
        response=response.response,
        row=response.row,
        record=source.from_row(response.row) if response.row else None,
    )


def get_rows_for_partition(
    client: BaseClient,
    table_name: str,
    partition_key_value: str,
    sort_key_starts: str = None,
    index: definitions.Index = definitions.Indexes.STANDARD,
    limit: int = 0,
) -> definitions.PagedRowResponse:
    """..."""
    attribute_names = {"#k0": index.partition_key}
    attribute_values = {":v0": {"S": str(partition_key_value)}}
    key_condition = "#k0=:v0"

    if index.sort_key:
        attribute_names["#k1"] = index.sort_key
        attribute_values[":v1"] = {"S": str(sort_key_starts)}
        key_condition += " AND begins_with ( #k1, :v1 )"

    request: dict = {
        "TableName": table_name,
        "ExpressionAttributeNames": attribute_names,
        "ExpressionAttributeValues": attribute_values,
        "KeyConditionExpression": key_condition,
    }

    if limit > 0:
        request["Limit"] = limit

    if index.name:
        request["IndexName"] = index.name

    paginator = client.get_paginator("query")
    rows: typing.List[dict] = []
    pages: typing.List[dict] = []

    for page in paginator.paginate(**request):
        pages.append(page)
        rows += page.get("Items") or []

    return definitions.PagedRowResponse(
        request=request,
        pages=tuple(pages or []),
        rows=tuple(rows or []),
    )


def get_records_for_partition(
    client: BaseClient,
    table_name: str,
    partition_key_value: str,
    sort_key_starts: str = None,
    index: definitions.Index = definitions.Indexes.STANDARD,
    record_classes: typing.List[typing.Type["recorder.Record"]] = None,
    limit: int = 0,
) -> recorder.PagedRecordResponse:
    """..."""
    result = get_rows_for_partition(
        client=client,
        table_name=table_name,
        partition_key_value=partition_key_value,
        sort_key_starts=sort_key_starts,
        index=index,
        limit=limit,
    )

    records = []
    for row in result.rows or []:
        match = next(
            (r.from_row(row) for r in (record_classes or []) if r.schema.matches(row)),
            None,
        )
        if match:
            records.append(match)

    return recorder.PagedRecordResponse(
        request=result.request,
        pages=result.pages,
        rows=result.rows,
        records=tuple(records or []),
    )


def read_entire_table(
    client: BaseClient,
    table_name: str,
    max_page_count: int = 100,
) -> definitions.ScannedRowResponse:
    """
    Reads entire table contents via a scan. Use with caution and only
    meant for debugging purposes. Do not use in production. Will only
    pull at-most the max specified number of pages before stopping to
    prevent extremely costly large scans. The return is a tuple where
    the first argument is whether or not the scan completed and returned
    all rows. If the page limit is hit this value will be false. The
    second argument is the returned list of raw dynamodb rows.
    """
    paginator = client.get_paginator("scan")
    rows: typing.List[dict] = []
    pages: typing.List[dict] = []
    completed = True
    request = {"TableName": table_name}
    for index, page in enumerate(paginator.paginate(**request)):
        if index > max_page_count:
            completed = False
            break

        pages.append(page)
        rows += page.get("Items") or []

    return definitions.ScannedRowResponse(
        completed=completed,
        request=request,
        pages=tuple(pages or []),
        rows=tuple(rows or []),
    )


def get_indexed_rows(
    client: BaseClient,
    table_name: str,
    partition_key_value: str,
    sort_key_value: typing.Optional[str],
    index: definitions.Index = definitions.Indexes.STANDARD,
    limit: int = 1,
) -> definitions.PagedRowResponse:
    """..."""
    attribute_names = {"#k0": index.partition_key}
    attribute_values = {":v0": {"S": str(partition_key_value)}}
    key_condition = "#k0=:v0"

    if index.sort_key and sort_key_value is not None:
        attribute_names["#k1"] = index.sort_key or ""
        attribute_values[":v1"] = {"S": str(sort_key_value)}
        key_condition = f"{key_condition} AND #k1=:v1"

    request: dict = {
        "TableName": table_name,
        "ExpressionAttributeNames": attribute_names,
        "ExpressionAttributeValues": attribute_values,
        "KeyConditionExpression": key_condition,
    }

    if index.name:
        request["IndexName"] = index.name

    if limit > 0:
        request["Limit"] = limit

    paginator = client.get_paginator("query")
    rows: typing.List[dict] = []
    pages: typing.List[dict] = []
    for page in paginator.paginate(**request):
        pages.append(page)
        rows += page.get("Items") or []

    return definitions.PagedRowResponse(
        request=request,
        pages=tuple(pages or []),
        rows=tuple(rows or []),
    )


def get_indexed_row(
    client: BaseClient,
    table_name: str,
    partition_key_value: str,
    sort_key_value: str,
    index: definitions.Index = definitions.Indexes.STANDARD,
) -> definitions.SingleRowResponse:
    """..."""
    result = get_indexed_rows(
        client=client,
        table_name=table_name,
        partition_key_value=partition_key_value,
        sort_key_value=sort_key_value,
        index=index,
        limit=1,
    )
    return definitions.SingleRowResponse(
        request=result.request,
        response=result.pages[0] if result.pages else {},
        row=result.rows[0] if result.rows else None,
    )


def get_indexed_records(
    client: BaseClient,
    table_name: str,
    source: "recorder.Record",
    index: definitions.Index = definitions.Indexes.STANDARD,
    limit: int = 1,
) -> recorder.PagedRecordResponse:
    """..."""
    columns = [
        source.schema.partition_key,
        source.schema.sort_key,
        # Common keys cannot be GSIs, so we ignore them here.
        *source.schema.columns,
    ]

    partition_column = next(
        (c for c in columns if c and index.partition_key in (c.name, c.key)), None
    )
    sort_column = next(
        (c for c in columns if c and index.sort_key in (c.name, c.key)), None
    )

    result = get_indexed_rows(
        client=client,
        table_name=table_name,
        partition_key_value=source.get_value_for(partition_column),
        sort_key_value=source.get_value_for(sort_column),
        index=index,
        limit=limit,
    )

    records = [source.from_row(row) for row in result.rows or []]
    return recorder.PagedRecordResponse(
        request=result.request,
        pages=result.pages,
        rows=result.rows,
        records=tuple(records or []),
    )


def get_indexed_record(
    client: BaseClient,
    table_name: str,
    source: "recorder.Record",
    index: definitions.Index = definitions.Indexes.STANDARD,
) -> recorder.SingleRecordResponse:
    """..."""
    result = get_indexed_records(
        client=client,
        table_name=table_name,
        source=source,
        index=index,
        limit=1,
    )
    return recorder.SingleRecordResponse(
        request=result.request,
        response=result.pages[0] if result.pages else {},
        row=result.rows[0] if result.rows else None,
        record=result.records[0] if result.records else None,
    )
