from datetime import date
from pipe_anchorages.anchorages_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, messages_table="SOURCE_TABLE", segments_table="SEGMENTS_TABLE_"):
        self.start_date = start_date
        self.end_date = end_date
        self.messages_table = messages_table
        self.segments_table = segments_table

def test_create_queries_1():
    # args = DummyOptions("2016-01-01", "2016-01-01")
    # assert list(create_queries(args, date(2016, 1, 1), date(2016, 1, 1))) == ["""
    # SELECT seg_id AS ident, ssvid, lat, lon, speed,
    #         CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    # FROM `SOURCE_TABLE`
    # WHERE date(timestamp) BETWEEN '2016-01-01' AND '2016-01-01'
      
    # """]

    args=DummyOptions(
        start_date='2012-05-01',
        end_date='2017-05-15',
    )
    assert create_queries(args) == ["""
    WITH

    destinations AS (
      SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
          CASE
            WHEN ARRAY_LENGTH(destinations) = 0 THEN NULL
            ELSE (SELECT MAX(value)
                  OVER (ORDER BY count DESC)
                  FROM UNNEST(destinations)
                  LIMIT 1)
            END AS destination
      FROM `SEGMENTS_TABLE_*`
      WHERE _TABLE_SUFFIX BETWEEN '20120501' AND '20150125'
    ),

    positions AS (
      SELECT ssvid, seg_id, lat, lon, timestamp, speed,
             date(timestamp) as table_suffix
        FROM `SOURCE_TABLE`
       WHERE date(timestamp) BETWEEN '2012-05-01' AND '2015-01-25'
         AND seg_id IS NOT NULL
         AND lat IS NOT NULL
         AND lon IS NOT NULL
         AND speed IS NOT NULL
    )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM positions
    JOIN destinations
    USING (seg_id, table_suffix)
    ""","""
    WITH

    destinations AS (
      SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
          CASE
            WHEN ARRAY_LENGTH(destinations) = 0 THEN NULL
            ELSE (SELECT MAX(value)
                  OVER (ORDER BY count DESC)
                  FROM UNNEST(destinations)
                  LIMIT 1)
            END AS destination
      FROM `SEGMENTS_TABLE_*`
      WHERE _TABLE_SUFFIX BETWEEN '20150126' AND '20170515'
    ),

    positions AS (
      SELECT ssvid, seg_id, lat, lon, timestamp, speed,
             date(timestamp) as table_suffix
        FROM `SOURCE_TABLE`
       WHERE date(timestamp) BETWEEN '2015-01-26' AND '2017-05-15'
         AND seg_id IS NOT NULL
         AND lat IS NOT NULL
         AND lon IS NOT NULL
         AND speed IS NOT NULL
    )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM positions
    JOIN destinations
    USING (seg_id, table_suffix)
    """]

