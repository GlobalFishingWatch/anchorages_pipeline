from datetime import date
from pipe_anchorages.port_events_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, input_table="SOURCE_TABLE", track_table="TRACK_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.input_table = input_table
        self.track_table = track_table
        self.fast_test = False
        self.start_padding = 3
        self.ssvid_filter = None

def test_create_queries_1():
    args = DummyOptions("2016-01-01", "2016-01-01")
    assert list(create_queries(args, date(2016, 1, 1), date(2016, 1, 1))) == ["""
    SELECT seg_id AS ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `SOURCE_TABLE*`
    WHERE _table_suffix BETWEEN '20160101' AND '20160101' 
      
    """]
    
def test_create_queries_2():
    args = DummyOptions("2012-5-01", "2017-05-15")
    assert list(create_queries(args, date(2012, 5, 1), date(2017, 5, 15))) == ["""
    SELECT seg_id AS ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `SOURCE_TABLE*`
    WHERE _table_suffix BETWEEN '20120501' AND '20150126' 
      
    """, 
    """
    SELECT seg_id AS ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `SOURCE_TABLE*`
    WHERE _table_suffix BETWEEN '20150127' AND '20170515' 
      
    """
        ]



