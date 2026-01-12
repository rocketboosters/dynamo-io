import datetime
import typing

from dynamo_io import definitions


def _as_bytes(raw: typing.Union[str, bytes]) -> bytes:
    """Convert the source to bytes to conform to dynamoDB outputs."""
    if isinstance(raw, bytes):
        return raw

    if isinstance(raw, str):
        return raw.encode()

    return raw


def unstringify(value: str, dtype: definitions.DynamoType) -> typing.Any:
    """Convert a string value to its native Python type based on the DynamoDB type.

    Args:
        value: The string value to convert.
        dtype: The DynamoDB type definition indicating the target type.

    Returns:
        The value converted to its native Python type (datetime, bool, float, int,
        bytes, etc.).
    """
    types = definitions.DynamoTypes

    if dtype.name == types.TIMESTAMP.name:
        return datetime.datetime.fromtimestamp(int(value), datetime.timezone.utc)

    if dtype.name == types.DATETIME.name:
        return datetime.datetime.fromisoformat("{}+00:00".format(value.rstrip("Z")))

    if dtype.name == types.DATE.name:
        return datetime.datetime.fromisoformat(value).date()

    if dtype.name == types.BOOLEAN.name:
        return bool(value)

    if dtype.name in (types.FLOAT.name, types.FLOAT_SET.name):
        return float(value)

    if dtype.name in (types.INTEGER.name, types.INTEGER_SET.name):
        return int(value)

    if dtype.name in (types.BYTES.name, types.BINARY_SET.name):
        return _as_bytes(value)

    return value


def _deserialize_map_column(
    raw: typing.Any,
    column: "definitions.MapColumn",
) -> typing.Dict[str, typing.Any]:
    """Deserialize a map column into a flattened dictionary."""
    children = typing.cast(typing.Tuple[definitions.ColumnType, ...], column.children)
    raw_children = typing.cast(typing.Dict[str, typing.Any], raw)
    return {
        child.name: deserialize(raw_children[child.name], child)
        for child in children
        if child and raw_children[child.name] not in (None, "")
    }


def deserialize(
    value: typing.Dict[str, typing.Union[str, list, dict, bool]],
    column: definitions.AnyColumnType | None = None,
) -> typing.Any:
    """Deserialize a DynamoDB value into its native Python representation.

    Args:
        value: The raw DynamoDB value dictionary containing type and value information.
        column: Optional column definition specifying the data type and structure.

    Returns:
        The deserialized Python value, or None if the column or value is None.
        Handles maps, sets, and scalar types according to the column definition.
    """
    if column is None or value[column.data_type.value] is None:
        return None

    raw = value[column.data_type.value]

    homogeneous_set_types = (
        definitions.DynamoTypes.BINARY_SET,
        definitions.DynamoTypes.FLOAT_SET,
        definitions.DynamoTypes.INTEGER_SET,
        definitions.DynamoTypes.STRING_SET,
    )

    if isinstance(column, definitions.MapColumn):
        return _deserialize_map_column(raw, column)
    elif column.data_type == definitions.DynamoTypes.MAP:
        lookups = definitions.TYPE_REVERSE_LOOKUP
        return {
            k: deserialize(
                data,
                typing.cast(
                    definitions.AnyColumnType,
                    definitions.Column(
                        name=k,
                        data_type=lookups[list(data.keys())[0]],
                    ),
                ),
            )
            for k, data in typing.cast(typing.Dict[str, dict], raw).items()
        }

    if column.data_type in homogeneous_set_types:
        return [unstringify(v, column.data_type) for v in typing.cast(list, raw)]

    return unstringify(typing.cast(str, raw), column.data_type)
