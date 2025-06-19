from datetime import datetime, timedelta, timezone as tz

from dateutil.parser import parse as dateutil_parse


def timestamp_from_datetime(dt: datetime, tzinfo: tz = tz.utc):
    """Converts a datetime to a unix timestamp.

    Args:
        dt: the datetime to convert.
        tzinfo: timezone.
    """
    return dt.replace(tzinfo=tzinfo).timestamp()


def datetime_from_timestamp(ts: float, tzinfo: tz = tz.utc):
    """Converts a unix timestamp to a datetime.

    Args:
        ts: the timestamp to convert.
        tzinfo: timezone.
    """
    return datetime.fromtimestamp(ts, tz=tzinfo)


def timestamp_from_string(d: str, tzinfo: tz = tz.utc):
    """Converts a sring into unix timestamp."""
    return timestamp_from_datetime(datetime_from_string(d, tzinfo))


def datetime_from_string(d: str, tzinfo: tz = tz.utc):
    """Converts a sring into datetime object."""
    return dateutil_parse(d).replace(tzinfo=tzinfo)


def list_of_days(start_date, end_date):
    """Returns list of days between start_date and end_date, excluding end_date."""
    return (start_date + timedelta(days=x) for x in range((end_date - start_date).days))

