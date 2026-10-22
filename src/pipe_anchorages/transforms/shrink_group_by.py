"""PTransform that groups records by key without repeating the key in every value."""

import datetime
import operator
import struct
from typing import Any, Callable, Iterable, Optional, get_type_hints

import apache_beam as beam

from gfw.common.beam.transforms import GroupBy


_EPOCH = datetime.datetime.fromtimestamp(0, datetime.UTC)
_MICROSECOND = datetime.timedelta(microseconds=1)


def _datetime_to_us(value: datetime.datetime) -> int:
    """Converts a timezone-aware datetime to integer microseconds since the epoch, exactly."""
    return (value - _EPOCH) // _MICROSECOND


def _us_to_datetime(value: int) -> datetime.datetime:
    """Converts integer microseconds since the epoch back to a UTC datetime."""
    return _EPOCH + datetime.timedelta(microseconds=value)


# (serializer, deserializer) of each field type that needs one to go through the shuffle as a
# value Beam's FastPrimitivesCoder encodes natively. NamedTuple fields get one from
# _packed_converters; fields of any other type pass unchanged.
_CONVERTERS: dict[type, tuple[Callable[[Any], Any], Callable[[Any], Any]]] = {
    datetime.datetime: (_datetime_to_us, _us_to_datetime),
}

# struct formats of the field types a NamedTuple can be packed from: fixed-size and exact.
_STRUCT_FORMATS: dict[Any, str] = {float: "d", bool: "?"}


class _PackedNamedTuple:
    """Packs a NamedTuple of fixed-size fields into bytes, and unpacks it.

    Packed, a NamedTuple of two floats takes 18 bytes, as two floats in the shuffled tuple do:
    a bytes tag and length, then 16 bytes, where a nested tuple would add its own tag and length
    to the floats' 9 bytes each (a tag, then 8 bytes).

    It keeps the format string, not a struct.Struct, since Beam pickles the transform and a
    struct.Struct can't be pickled; struct caches the compiled format.
    """

    def __init__(self, record_type: Any, fmt: str) -> None:
        self._record_type = record_type
        self._fmt = fmt

    def serialize(self, value: Any) -> bytes:
        return struct.pack(self._fmt, *value)

    def deserialize(self, data: bytes) -> Any:
        return self._record_type._make(struct.unpack(self._fmt, data))


def _packed_converters(
    field_type: Any,
) -> Optional[tuple[Callable[[Any], Any], Callable[[Any], Any]]]:
    """Returns the (serializer, deserializer) packing a NamedTuple field type into bytes.

    Only a NamedTuple whose fields are all annotated with a type in _STRUCT_FORMATS can be
    packed; for any other type, returns None.
    """
    if not hasattr(field_type, "_fields"):
        return None

    hints = get_type_hints(field_type)
    formats = [_STRUCT_FORMATS.get(hints.get(name)) for name in field_type._fields]
    if not formats or None in formats:
        return None

    packer = _PackedNamedTuple(field_type, "<" + "".join(formats))
    return packer.serialize, packer.deserialize


class ShrinkGroupBy(GroupBy):
    """Groups records by key, omitting the key fields from the shuffled values.

    Works like GroupBy over a NamedTuple record type, but shuffles each record shrunken:
    a plain tuple of its non-key fields, with the field types in _CONVERTERS converted to
    primitives (datetimes become integer microseconds since epoch), and NamedTuple fields of floats
    and bools (like LatLon) packed into bytes. The records come back rebuilt from the key and
    the shrunken values.

    Field types are read from the record type's annotations. The record type is a plain
    NamedTuple — no base class or decorator required. The step label comes from GroupBy,
    like ``GroupPositionsBySsvidAndVessel_Id``.

    Example:
        .. code-block:: python

            records | ShrinkGroupBy("ssvid", "vessel_id", record_type=VisitLocationRecord)

    Args:
        *fields:
            Names of the record fields to group by. The key carries them, so they are left
            out of the shuffled values.

        record_type:
            A NamedTuple class whose records will be shrunken and restored.

        elements:
            A human-readable label describing the grouped elements (e.g., ``Positions``).
            It is used to generate the step label.

        **computed:
            Extra keys computed from each record, like a date from its timestamp. They go in
            the key after the fields, and the record keeps the fields they come from.

    Input: a PCollection of records of `record_type`.
    Output: a PCollection of (key, list[record]) pairs. The key is a tuple of the field values
    and then the computed values, or a single value if there is only one key, like GroupBy's.
    """

    def __init__(
        self,
        *fields: str,
        record_type: type,
        elements: str = "",
        **computed: Callable[[Any], Any],
    ) -> None:
        self._validate(record_type, fields, computed)

        super().__init__(*fields, elements=elements, dict_fields=False, **computed)
        self._record_type = record_type
        self._single_key = len(fields) + len(computed) == 1

        self._key_indices = tuple(record_type._fields.index(name) for name in fields)
        self._key_getters: tuple[Callable[[Any], Any], ...] = (
            *(operator.itemgetter(i) for i in self._key_indices),
            *computed.values(),
        )

        self._keep_indices = tuple(
            i for i, name in enumerate(record_type._fields)
            if name not in fields
        )

        hints = get_type_hints(record_type)
        self._serializers = []
        self._deserializers = []

        for pos, i in enumerate(self._keep_indices):
            field_type = hints.get(record_type._fields[i])
            converters = _CONVERTERS.get(field_type)
            if converters is None:
                converters = _packed_converters(field_type)
            if converters is None:
                continue

            serializer, deserializer = converters
            # Shrunken tuples are indexed by position; rebuilt records by field index.
            self._serializers.append((pos, serializer))
            self._deserializers.append((i, deserializer))

    @staticmethod
    def _validate(
        record_type: type, fields: tuple[str, ...], computed: dict[str, Any],
    ) -> None:
        """Checks that record_type is a NamedTuple, with every key field, and there is a key."""
        if not hasattr(record_type, "_fields"):
            raise TypeError(f"{record_type.__name__} must be a NamedTuple with _fields")

        if not fields and not computed:
            raise ValueError("At least one field or computed key is required")

        for name in fields:
            if name not in record_type._fields:
                raise ValueError(
                    f"Key {name!r} is not declared in {record_type.__name__}._fields"
                )

    # Beam infers coders from the hints of the methods it maps: the values stay Any so it keeps
    # FastPrimitivesCoder, and unshrink takes an Iterable, which is what GroupByKey outputs.
    def shrink(self, record: Any) -> tuple[Any, tuple[Any, ...]]:
        """Keys a record by its grouping key and shrinks it to its non-key fields.

        Args:
            record:
                A record of `record_type`.

        Returns:
            A (key, shrunken_tuple) pair. The shrunken tuple holds the non-key fields as
            primitives.
        """
        key = tuple([get(record) for get in self._key_getters])
        if self._single_key:
            key = key[0]

        values = [record[i] for i in self._keep_indices]
        for pos, serialize in self._serializers:
            values[pos] = serialize(values[pos])

        return key, tuple(values)

    def unshrink(self, item: tuple[Any, Iterable[tuple[Any, ...]]]) -> tuple[Any, list[Any]]:
        """Rebuilds a group's records, from the key they were grouped by.

        The key fields are the same for the whole group, so they are placed once, in a list that
        each record then fills with its own values. The computed keys are not used.

        Args:
            item:
                A (key, shrunken_values) pair.

        Returns:
            A (key, records) pair.
        """
        key, shrunken_values = item
        key_values = key
        if self._single_key:
            key_values = (key,)

        values = [None] * len(self._record_type._fields)
        for i, value in zip(self._key_indices, key_values, strict=False):
            values[i] = value

        records = []
        for shrunken in shrunken_values:
            # Append primitive values.
            for i, value in zip(self._keep_indices, shrunken, strict=True):
                values[i] = value

            # Append serialized values.
            for i, deserialize in self._deserializers:
                values[i] = deserialize(values[i])

            records.append(self._record_type._make(values))

        return key, records

    def expand(self, records: beam.PCollection) -> beam.PCollection:
        """Shrinks records, groups them by key, and restores the original records.

        Args:
            records:
                A PCollection of records of `record_type`.

        Returns:
            A PCollection of (key, list[record]) pairs.
        """
        return (
            records
            | "Shrink" >> beam.Map(self.shrink)
            | "GroupByKey" >> beam.GroupByKey()
            | "Unshrink" >> beam.Map(self.unshrink)
        )
