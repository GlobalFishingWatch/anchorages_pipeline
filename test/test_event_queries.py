from pipe_anchorages.port_events_pipeline import create_queries

class DummyOptions(object):
    def __init__(self, start_date, end_date, input_table="SOURCE_TABLE", track_table="TRACK_TABLE"):
        self.start_date = start_date
        self.end_date = end_date
        self.input_table = input_table
        self.track_table = track_table
        self.fast_test = False
        self.start_padding = 3

def test_create_queries_1():
    args = DummyOptions("2016-01-01", "2016-01-01")
    print(list(create_queries(args))[0])
    assert list(create_queries(args)) == ["""
    with 

    track_id as (
      select track_id, seg_id as aug_seg_id
      from `TRACK_TABLE`
      cross join unnest (seg_ids) as seg_id
    ),

    base as (
        select *,
               concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
        from `SOURCE_TABLE*`
        where _table_suffix between '20151229' and '20160101' 
    ),

    source as (
        select base.*, track_id
        from base
        join (select * from track_id)
        using (aug_seg_id)
    )


    select track_id as ident, lat, lon, timestamp, speed from 
       source
    """]
    
def test_create_queries_2():
    args = DummyOptions("2012-5-01", "2017-05-15")
    assert list(create_queries(args)) == ["""
    with 

    track_id as (
      select track_id, seg_id as aug_seg_id
      from `TRACK_TABLE`
      cross join unnest (seg_ids) as seg_id
    ),

    base as (
        select *,
               concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
        from `SOURCE_TABLE*`
        where _table_suffix between '20120428' and '20150120' 
    ),

    source as (
        select base.*, track_id
        from base
        join (select * from track_id)
        using (aug_seg_id)
    )


    select track_id as ident, lat, lon, timestamp, speed from 
       source
    """, 
    """
    with 

    track_id as (
      select track_id, seg_id as aug_seg_id
      from `TRACK_TABLE`
      cross join unnest (seg_ids) as seg_id
    ),

    base as (
        select *,
               concat(seg_id, '-', format_date('%F', date(timestamp))) aug_seg_id
        from `SOURCE_TABLE*`
        where _table_suffix between '20150121' and '20170515' 
    ),

    source as (
        select base.*, track_id
        from base
        join (select * from track_id)
        using (aug_seg_id)
    )


    select track_id as ident, lat, lon, timestamp, speed from 
       source
    """
        ]



