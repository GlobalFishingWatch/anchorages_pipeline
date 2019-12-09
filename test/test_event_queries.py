from pipe_anchorages.port_events_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, input_table="SOURCE_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.input_table = input_table
        self.fast_test = False
        self.start_padding = 3

def test_create_queries_1():
    args = DummyOptions("2016-01-01", "2016-01-01")
    assert list(create_queries(args)) == ["""
    select vessel_id as ident, lat, lon, timestamp, speed from 
       `SOURCE_TABLE*`
       where _table_suffix between '20151229' and '20160101' 
    """]
    
def test_create_queries_2():
    args = DummyOptions("2012-5-01", "2017-05-15")
    print(list(create_queries(args))[1])
    assert list(create_queries(args)) == ["""
    select vessel_id as ident, lat, lon, timestamp, speed from 
       `SOURCE_TABLE*`
       where _table_suffix between '20120428' and '20150120' 
    """, 
    """
    select vessel_id as ident, lat, lon, timestamp, speed from 
       `SOURCE_TABLE*`
       where _table_suffix between '20150121' and '20170515' 
    """
        ]