message_schema = {
    "fields": [
        {"name": "identifier", "type": "STRING", "mode": "NULLABLE"},
        {
            "description": "The timestamp when the message was received.",
            "mode": "NULLABLE",
            "name": "timestamp",
            "type": "TIMESTAMP",
        },
        {
            "description": "The longitude included in the message.",
            "mode": "NULLABLE",
            "name": "lon",
            "type": "FLOAT",
        },
        {
            "description": "The latitude included in the message.",
            "mode": "NULLABLE",
            "name": "lat",
            "type": "FLOAT",
        },
        {
            "description": "The speed in knots included in the message.",
            "mode": "NULLABLE",
            "name": "speed",
            "type": "FLOAT",
        },
        {
            "description": "Could this message be a gap end.",
            "mode": "NULLABLE",
            "name": "is_possible_gap_end",
            "type": "BOOL",
        },
        {
            "description": "If we are near an anchorage, what is it's S2 ID.",
            "mode": "NULLABLE",
            "name": "port_s2id",
            "type": "STRING",
        },
        {
            "description": "If we are near an anchorage, how far away is it.",
            "mode": "NULLABLE",
            "name": "port_dist",
            "type": "FLOAT",
        },
        {
            "description": "If we are near an anchorage, what is its mean longitude.",
            "mode": "NULLABLE",
            "name": "port_lon",
            "type": "FLOAT",
        },
        {
            "description": "If we are near an anchorage, what is its mean latitude.",
            "mode": "NULLABLE",
            "name": "port_lat",
            "type": "FLOAT",
        },
    ]
}
