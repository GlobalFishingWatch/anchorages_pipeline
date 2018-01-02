from pipe_anchorages.port_events_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, input_table="SOURCE_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.input_table = input_table
        self.fast_test = False


def test_create_queries_1():
    args = DummyOptions("2016-01-01", "2016-01-01")
    assert list(create_queries(args)) == ["""
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                        TIMESTAMP('2015-12-31'), TIMESTAMP('2016-01-01')) 
    """]
    
def test_create_queries_2():
    args = DummyOptions("2012-5-01", "2017-05-15")
    # print list(create_queries(args))[0]
    # print list(create_queries(args))[1]
    assert list(create_queries(args)) == ["""
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                        TIMESTAMP('2012-04-30'), TIMESTAMP('2015-01-24')) 
    """, 
    """
    SELECT mmsi, lat, lon, timestamp, destination, speed FROM   
      TABLE_DATE_RANGE([world-fishing-827:SOURCE_TABLE.], 
                        TIMESTAMP('2015-01-25'), TIMESTAMP('2017-05-15')) 
    """
        ]