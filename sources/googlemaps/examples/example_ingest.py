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
        {
            "table": {
                "source_table": "geocoder",
                "destination_catalog": "googlemaps",
                "destination_schema": "geocoding",
                "destination_table": "geocodes",
                "table_configuration": {
                    "address": "Köpenicker Str. 41, 10179 Berlin",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["place_id"]
                },
            }
        },
        # {
        #     "table": {
        #         "source_table": "places",
        #         "destination_catalog": "googlemaps",
        #         "destination_schema": "places",
        #         "destination_table": "gas_stations_munich",
        #         "table_configuration": {
        #             "text_query": "Gas station in Munich",
        #             "scd_type": "APPEND_ONLY",
        #             "language_code": "en",
        #             "max_result_count": "20",
        #             "included_type": "gas_station",
        #             "min_rating": "4.0",
        #             "region_code": "DE"
        #         },
        #     },
        # },
        {
            "table": {
                "source_table": "places",
                "destination_catalog": "googlemaps",
                "destination_schema": "places",
                "destination_table": "gas_stations_berlin",
                "table_configuration": {
                    "text_query": "Gas station in Berlin",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["id"],
                    "language_code": "en",
                    "max_result_count": "20",
                    "included_type": "gas_station",
                    "min_rating": "4.0",
                    "region_code": "DE"
                },
            }
        },
        {
            "table": {
                "source_table": "distance_matrix",
                "destination_catalog": "googlemaps",
                "destination_schema": "distance_matrix",
                "destination_table": "distances",
                "table_configuration": {
                    "origins": ["Köpenicker Str. 41, 10179 Berlin"],
                    "destinations": ["Kurfürstendamm 1, 10785 Berlin"],
                    "mode": "driving",
                    "departure_time": "now",
                    "traffic_model": "best_guess",
                    "units": "metric",
                    "language_code": "en"
                }
            }
        }
    ],
}


# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)