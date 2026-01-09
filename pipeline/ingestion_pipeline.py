# pylint: disable=no-member
import json
from dataclasses import dataclass
from typing import List
from pyspark import pipelines as sdp
from pyspark.sql.functions import col, expr
from libs.spec_parser import SpecParser


@dataclass
class SdpTableConfig:  # pylint: disable=too-many-instance-attributes
    """SDP configuration to ingest a table."""

    source_table: str
    destination_table: str
    view_name: str
    table_config: dict[str, str]
    primary_keys: List[str]
    sequence_by: str
    scd_type: str
    with_deletes: bool = False


def _create_cdc_table(
    spark, connection_name: str, config: SdpTableConfig
) -> None:
    """Create CDC table using streaming and apply_changes

    """

    @sdp.view(name=config.view_name)
    def v():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        sequence_by=col(config.sequence_by),
        stored_as_scd_type=config.scd_type,
    )

    # Delete flow - only enabled for cdc_with_deletes ingestion type
    if config.with_deletes:
        # Use view_name base (which is destination-based) for uniqueness
        delete_view_name = config.view_name.replace("_staging", "_delete_staging")

        @sdp.view(name=delete_view_name)
        def delete_view():
            return (
                spark.readStream.format("lakeflow_connect")
                .option("databricks.connection", connection_name)
                .option("tableName", config.source_table)
                .option("isDeleteFlow", "true")
                .options(**config.table_config)
                .load()
            )

        sdp.apply_changes(
            target=config.destination_table,
            source=delete_view_name,
            keys=config.primary_keys,
            sequence_by=col(config.sequence_by),
            stored_as_scd_type=config.scd_type,
            apply_as_deletes=expr("true"),
            name=delete_view_name + "_delete_flow",
        )


def _create_snapshot_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create snapshot table using batch read and apply_changes_from_snapshot"""

    @sdp.view(name=config.view_name)
    def snapshot_view():
        return (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )

    sdp.create_streaming_table(name=config.destination_table)
    sdp.apply_changes_from_snapshot(
        target=config.destination_table,
        source=config.view_name,
        keys=config.primary_keys,
        stored_as_scd_type=config.scd_type,
    )


def _create_append_table(spark, connection_name: str, config: SdpTableConfig) -> None:
    """Create append table using streaming without apply_changes"""

    sdp.create_streaming_table(name=config.destination_table)

    @sdp.append_flow(name=config.view_name, target=config.destination_table)
    def af():
        return (
            spark.readStream.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", config.source_table)
            .options(**config.table_config)
            .load()
        )


def _get_table_metadata(
    spark, connection_name: str, table_list: list[str], table_configs: dict[str, str]
) -> dict:
    """Get table metadata (primary_keys, cursor_field, ingestion_type etc.)"""
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "_lakeflow_metadata")
        .option("tableNameList", ",".join(table_list))
        .option("tableConfigs", json.dumps(table_configs))
        .load()
    )
    metadata = {}
    for row in df.collect():
        table_metadata = {}
        if row["primary_keys"] is not None:
            table_metadata["primary_keys"] = row["primary_keys"]
        if row["cursor_field"] is not None:
            table_metadata["cursor_field"] = row["cursor_field"]
        if row["ingestion_type"] is not None:
            table_metadata["ingestion_type"] = row["ingestion_type"]
        metadata[row["tableName"]] = table_metadata
    return metadata


def ingest(spark, pipeline_spec: dict) -> None:
    """Ingest a list of tables.

    Supports multiple objects with the same source_table but different configurations
    (e.g., places for Berlin vs places for Munich). Each object gets a unique view
    based on its destination_table name.
    """

    # parse the pipeline spec
    spec = SpecParser(pipeline_spec)
    connection_name = spec.connection_name()

    # Get unique source tables for metadata fetching (avoids duplicate API calls)
    unique_source_tables = spec.get_unique_source_tables()

    # Get table_configurations for unique source tables (for metadata API)
    # Note: We use the first configuration found for each source table for metadata.
    # Individual object configurations are applied during ingestion.
    table_configs = spec.get_table_configurations()
    metadata = _get_table_metadata(spark, connection_name, unique_source_tables, table_configs)

    def _ingest_object(index: int) -> None:
        """Helper function to ingest a single object by index."""
        source_table = spec.get_source_table_by_index(index)
        dest_table_name = spec.get_destination_table_by_index(index)

        # Get metadata from source table (same for all objects with this source)
        table_metadata = metadata.get(source_table, {})
        primary_keys = table_metadata.get("primary_keys")
        cursor_field = table_metadata.get("cursor_field")
        ingestion_type = table_metadata.get("ingestion_type", "cdc")

        # Use destination table name for view to ensure uniqueness
        # This allows multiple objects with the same source_table
        view_name = dest_table_name + "_staging"

        # Get object-specific configuration by index
        table_config = spec.get_table_configuration_by_index(index)
        destination_table = spec.get_full_destination_table_name_by_index(index)

        # Override parameters with spec values if available (by index)
        primary_keys = spec.get_primary_keys_by_index(index) or primary_keys
        sequence_by = spec.get_sequence_by_by_index(index) or cursor_field
        scd_type_raw = spec.get_scd_type_by_index(index)
        if scd_type_raw == "APPEND_ONLY":
            ingestion_type = "append"
        scd_type = "2" if scd_type_raw == "SCD_TYPE_2" else "1"

        config = SdpTableConfig(
            source_table=source_table,
            destination_table=destination_table,
            view_name=view_name,
            table_config=table_config,
            primary_keys=primary_keys,
            sequence_by=sequence_by,
            scd_type=scd_type,
            with_deletes=(ingestion_type == "cdc_with_deletes"),
        )

        if ingestion_type in ("cdc", "cdc_with_deletes"):
            _create_cdc_table(
                spark,
                connection_name,
                config
            )
        elif ingestion_type == "snapshot":
            _create_snapshot_table(spark, connection_name, config)
        elif ingestion_type == "append":
            _create_append_table(spark, connection_name, config)

    # Iterate over all objects by index to support multiple same-source tables
    for i in range(spec.get_object_count()):
        _ingest_object(i)
