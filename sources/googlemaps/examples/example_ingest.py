from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "googlemaps"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of tables to ingest
#     └── table
#         ├── source_table (required): The table name in the source system
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (optional)
#             ├── scd_type: "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys: List of columns to override connector's default keys
#             └── (other options): See source connector's README
# =============================================================================

# Please update the spec below to configure your ingestion pipeline.

pipeline_spec = {
    "connection_name": "googlemaps",
    "objects": [
        # Minimal config: just specify the source table
        # {
        #     "table": {
        #         "source_table": "places",
        #         "table_configuration": {
        #             "text_query": "Restaurants in Berlin",
        #             "scd_type": "APPEND_ONLY"
        #         },
        #     }
        # },
        # Full config: customize destination and behavior
        {
            "table": {
                "source_table": "places",
                "destination_catalog": "googlemaps",
                "destination_schema": "places",
                "destination_table": "restaurants_berlin",
                "table_configuration": {
                    "text_query": "Restaurants in Berlin",
                    "scd_type": "APPEND_ONLY",
                    "primary_keys": ["id"],
                    "language_code": "en",
                    "max_result_count": "20",
                    "included_type": "restaurant",
                    "min_rating": "4.0",
                    "open_now": "true",
                    "region_code": "DE"
                },
            }
        },
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)