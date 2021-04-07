from __future__ import absolute_import, print_function, division

import datetime
import logging
import pytz

import apache_beam as beam
from apache_beam import Filter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from . import common as cmn
from .transforms.source import QuerySource
from .transforms.create_tagged_anchorages import CreateTaggedAnchorages
from .transforms.create_in_out_events import CreateInOutEvents
from .transforms.sink import EventSink, EventStateSink
from .options.port_events_options import PortEventsOptions
from .schema.port_event import build_event_state_schema 


def create_queries(args, start_date, end_date):
    template = """
    SELECT seg_id AS ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp,
    FROM `{table}*`
    WHERE _table_suffix BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}' 
      AND ssvid in ("636016998", "663037000", "263403770", "210631000", "212373000", "214182713", "214182714", 
      "214182723", "214182732", "224100140", "224224000", "224303000", "224431000", "224449000", "224531000", 
      "224580000", "224587000", "224709000", "224727000", "224745000", "225458000", "225459000", "225461000", 
      "226180000", "227549000", "227550000", "228066900", "228071900", "228128000", "228167000", "228168000", 
      "228280000", "244700519", "244790145", "263403770", "277312000", "277327000", "277348000", "306034000", 
      "306094000", "306097000", "306106000", "306117000", "306118000", "306120000", "306752000", "306796000", 
      "306862000", "306872000", "308228000", "308541000", "309165000", "309193000", "309361000", "309689000", 
      "309716000", "309841000", "311000682", "311000709", "311000712", "311000713", "311000788", "311000791", 
      "311000804", "311000855", "311009400", "311009500", "311009600", "311040500", "311042400", "312004000", 
      "312051000", "312191000", "312391000", "312507000", "312547000", "312590000", "312599000", "312756000", 
      "312797000", "312867000", "312955000", "325103700", "325110000", "325955000", "325965000", "325967000", 
      "325981000", "332001000", "341864000", "341878000", "351336000", "351698000", "352545000", "353056000", 
      "353154000", "353306000", "353919000", "353982000", "354753000", "355794000", "355855000", "356262000", 
      "356702000", "356728000", "357919000", "357993000", "359003000", "359004000", "359101000", "359102000", 
      "370078000", "370095000", "371322000", "371812000", "371996000", "374505000", "374573000", "376715000", 
      "412209620", "412209650", "412550006", "412660240", "412699120", "518100027", "518332000", "529520000", 
      "535096772", "566049000", "613003548", "613003611", "617095000", "617106000", "627072000", "627074000", 
      "627084000", "627163000", "627211000", "627762000", "627851000", "627887000", "627996000", "627997000", 
      "636013068", "636013073", "636014240", "636014241", "636015811", "636016700", "636016998", "636017545", 
      "636017546", "636018457", "636083838", "663037000", "663136000", "664589000"
      )

    -- AND vessel_id = '0d59d9f05-5189-96fb-f1d9-28d0294e4924' -- REMOVE
    """
    start_window = start_date
    shift = 1000
    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(table=args.input_table, start=start_window, end=end_window)
        if args.fast_test:
            query += 'LIMIT 100000'
        yield query
        start_window = end_window + datetime.timedelta(days=1)


anchorage_query = 'SELECT lat as anchor_lat, lon as anchor_lon, s2id as anchor_id, label FROM `{}`'


def run(options):

    known_args = options.view_as(PortEventsOptions)
    cloud_options = options.view_as(GoogleCloudOptions)

    start_date = datetime.datetime.strptime(known_args.start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(known_args.end_date, '%Y-%m-%d').date()

    p = beam.Pipeline(options=options)

    config = cmn.load_config(known_args.config)

    queries = create_queries(known_args, start_date, end_date)

    sources = [(p | "Read_{}".format(i) >> 
                        beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(query=x, use_standard_sql=True)))
                        for (i, x) in enumerate(queries)]

    tagged_records = (sources
        | beam.Flatten()
        | cmn.CreateVesselRecords(destination=None)
        | cmn.CreateTaggedRecords(min_required_positions=1, thin=False)
        )


    # TODO: this assumes track id, need to revert to seg_id try that version
    # TODO: then think about stitching together across segments.


    prev_date = start_date - datetime.timedelta(days=1)
    state_table = f'{known_args.state_table}{prev_date:%Y%m%d}'

    # TODO: make into separate function
    client = bigquery.Client(project=cloud_options.project)
    try:
        client.get_table(state_table) 
        state_table_exists = True
    except NotFound:
        state_table_exists = False

    if state_table_exists:
        initial_states = (p
            | beam.io.Read(beam.io.gcp.bigquery.BigQuerySource(table=state_table, validate=True))
            | beam.Map(lambda x : (x['seg_id'], x))
        )
    else:
        logging.warning('No state table found, using empty table')
        initial_states = (p | beam.Create([]))

    tagged_records = {'records' : tagged_records, 'state' : initial_states} | beam.CoGroupByKey()

    anchorages = (p
        # TODO: why not just use BigQuerySource?
        | 'ReadAnchorages' >> QuerySource(anchorage_query.format(known_args.anchorage_table),
                                           use_standard_sql=True)
        | CreateTaggedAnchorages()
        )

    tagged_records, states = (tagged_records
        | CreateInOutEvents(anchorages=anchorages,
                            anchorage_entry_dist=config['anchorage_entry_distance_km'], 
                            anchorage_exit_dist=config['anchorage_exit_distance_km'], 
                            stopped_begin_speed=config['stopped_begin_speed_knots'],
                            stopped_end_speed=config['stopped_end_speed_knots'],
                            min_gap_minutes=config['minimum_port_gap_duration_minutes'],
                            start_date=start_date, 
                            end_date=end_date)
        )

    (tagged_records 
        | "writeInOutEvents" >> EventSink(table=known_args.output_table, 
                                          temp_location=cloud_options.temp_location,
                                          project=cloud_options.project)
        )

    states | 'WriteState' >> EventStateSink(known_args.state_table, 
                                           temp_location=cloud_options.temp_location,
                                            project=cloud_options.project)


    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


