from types import SimpleNamespace

from pipe_anchorages.hooks import update_table_metadata_hook
from pipe_anchorages.pipelines.anchorage_locations.table_config import (
    AnchorageLocationsTableConfig,
    AnchorageLocationsTableDescription,
)


def test_update_table_metadata_hook_sets_description_and_labels(bq_clients, bq_client_factory):
    table_config = AnchorageLocationsTableConfig(
        table_id="project.dataset.table",
        description=AnchorageLocationsTableDescription(version="1.0.0"),
    )
    labels = {"environment": "development"}
    pipeline = SimpleNamespace(cloud_options=SimpleNamespace(project="test-project"))

    update_table_metadata_hook(table_config, labels, bq_client_factory)(pipeline)

    (client,) = bq_clients
    (table,), _ = client.get_table.call_args
    assert str(table) == "project.dataset.table"
    (updated, fields), _ = client.update_table.call_args
    assert fields == ["description", "labels"]
    assert updated.description == table_config.description.render()
    assert updated.labels == labels
