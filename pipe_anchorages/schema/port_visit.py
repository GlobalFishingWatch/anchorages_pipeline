port_visit_schema = {
    "fields": [
        {
            "name": "visit_id",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "Unique ID for this visit"
        },
        {
            "name": "vessel_id",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "`vessel_id` of the track this visit was found on"
        },
        {
            "name": "ssvid",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "`ssvid` of the vessel involved in the visit."
            "N.B. Some `ssvid` may be associated with multiple tracks",
        },
        {
            "name": "start_timestamp",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
            "description": "timestamp at which vessel crossed into the anchorage",
        },
        {
            "name": "start_lat",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "latitude of vessel at `start_timestamp`"
        },
        {
            "name": "start_lon",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "longitude of vessel at `start_timestamp`"
        },
        {
            "name": "start_anchorage_id",
            "type": "STRING",
            "description": "`anchorage_id` of anchorage where vessel entered port",
        },
        {
            "name": "end_timestamp",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
            "description": "timestamp at which vessel crossed out the anchorage.",
        },
        {
            "name": "end_lat",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "latitude of vessel at `end_timestamp`"
        },
        {
            "name": "end_lon",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "description": "longitude of vessel at `end_timestamp`"
        },
        {
            "name": "duration_hrs",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "duration of visit in hours"
        },
        {
            "name": "end_anchorage_id",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "longitude of vessel at `end_timestamp`"
        },
        {
            "name": "confidence",
            "mode": "NULLABLE",
            "type": "INTEGER",
            "description": """How confident are we that this is a real visit based on components of the visits:
                1 -> no stop or gap; only an entry and/or exit
                2 -> only stop and/or gap; no entry or exit
                3 -> port entry or exit with stop and/or gap
                4 -> port entry and exit with stop and/or gap""",
        },
        {
            "name": "events",
            "mode": "REPEATED",
            "type": "RECORD",
            "description": "sequence of port events that occurred during visit",
            "fields": [
                {
                    "name": "seg_id",
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "description": "The segment id belonging to the vessel."
                },
                {
                    "name": "timestamp",
                    "mode": "NULLABLE",
                    "type": "TIMESTAMP",
                    "description": "The timestamp when the message was received."
                },
                {
                    "name": "lat",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The latitude included in the message."
                },
                {
                    "name": "lon",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The longitude included in the message."
                },
                {
                    "name": "vessel_lat",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The latitude of the vessel."
                },
                {
                    "name": "vessel_lon",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The longitude of the vessel."
                },
                {
                    "name": "anchorage_id",
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "description": "The id of the anchorage."
                },
                {
                    "name": "event_type",
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "description": "The event type."
                },
            ],
        },
    ]
}
