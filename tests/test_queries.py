from pipe_anchorages.queries import MessagesQuery


def test_messages_query_renders_by_ssvid_without_ssvid_filter():
    query = MessagesQuery(
        source_messages="SOURCE_TABLE",
        start_date="2016-01-01",
        end_date="2016-01-02",
    )
    expected = """
SELECT
    ssvid AS ident,
    lat,
    lon,
    CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp,
    destination,
    speed
FROM
    `SOURCE_TABLE`
WHERE
    date(timestamp) >= '2016-01-01'
    AND date(timestamp) < '2016-01-02'
    AND seg_id IS NOT NULL
    AND lat IS NOT NULL
    AND lon IS NOT NULL
    AND speed IS NOT NULL
"""
    assert query.render() == expected.lstrip("\n")


def test_messages_query_renders_the_ident_field_destination_and_ssvid_filter():
    query = MessagesQuery(
        source_messages="SOURCE_TABLE",
        start_date="2016-01-01",
        end_date="2016-01-02",
        ident_field="seg_id",
        include_destination=False,
        ssvid_filter="'111', '222'",
    )

    rendered = query.render()

    assert rendered.startswith("SELECT\n    seg_id AS ident,\n")
    assert "destination" not in rendered
    assert rendered.endswith("    AND speed IS NOT NULL\n    AND ssvid IN ('111', '222')\n")
