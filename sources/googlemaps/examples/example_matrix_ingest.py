"""
Google Maps Matrix-Based Ingestion Pipeline Example.

This example demonstrates the table_matrix feature which allows you to generate
multiple destination tables from a single connector table using parameter matrices.

Instead of writing 10+ repetitive table configurations for different cities,
define a single matrix and let the framework expand it automatically.

Benefits:
- Reduce pipeline spec size by 90%+ for multi-parameter use cases
- Maintain consistency across all generated tables
- Easy to add/remove cities or change common settings
- DRY principle - Don't Repeat Yourself
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from libs.table_matrix import expand_pipeline_spec, generate_city_parameters

SOURCE_NAME = "googlemaps"

# =============================================================================
# CONFIGURATION
# =============================================================================

# Define German cities to search for places
GERMAN_CITIES = [
    "Berlin, Germany",
    "Munich, Germany",
    "Hamburg, Germany",
    "Frankfurt, Germany",
    "Cologne, Germany",
    "Stuttgart, Germany",
    "Düsseldorf, Germany",
    "Leipzig, Germany",
    "Dortmund, Germany",
    "Essen, Germany",
]

# You can easily switch to other regions:
# US_CITIES = ["New York, USA", "Los Angeles, USA", "Chicago, USA", ...]
# UK_CITIES = ["London, UK", "Manchester, UK", "Birmingham, UK", ...]

# =============================================================================
# MATRIX-BASED PIPELINE SPEC
# =============================================================================
#
# The 'matrix' object type allows you to define a parameter matrix that gets
# expanded into multiple 'table' objects automatically.
#
# Matrix structure:
# {
#     "matrix": {
#         "source_table": "places",              # Connector table name
#         "parameters": [                         # List of varying parameters
#             {"location_address": "Berlin", ...},
#             {"location_address": "Munich", ...},
#         ],
#         "common_config": {                      # Shared configuration
#             "included_types": "restaurant",
#             "scd_type": "SCD_TYPE_1",
#         },
#         "destination": {                        # Destination template
#             "catalog": "my_catalog",
#             "schema": "my_schema",
#             "table_template": "restaurants_{location_address_slug}",
#         }
#     }
# }
#
# Template variables in table_template:
#   - {param_name}      - Raw parameter value
#   - {param_name_slug} - Slugified value (lowercase, underscores only)
#
# Example: "restaurants_{location_address_slug}" with "Berlin, Germany"
#       -> "restaurants_berlin_germany"
#
# =============================================================================

pipeline_spec_with_matrix = {
    "connection_name": "googlemaps",
    "objects": [
        # ---------------------------------------------------------------------
        # Regular table: Geocode our office location
        # ---------------------------------------------------------------------
        {
            "table": {
                "source_table": "geocoder",
                "destination_catalog": "googlemaps",
                "destination_schema": "geocoding",
                "destination_table": "office_location",
                "table_configuration": {
                    "address": "Köpenicker Str. 41, 10179 Berlin",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["place_id"],
                }
            }
        },
        # ---------------------------------------------------------------------
        # MATRIX: Generate restaurant tables for all German cities
        # This single matrix definition expands to 10 separate tables!
        # ---------------------------------------------------------------------
        {
            "matrix": {
                "source_table": "places",
                "parameters": generate_city_parameters(
                    GERMAN_CITIES,
                    radius="3000",  # 3km radius
                ),
                "common_config": {
                    "included_types": "restaurant",
                    "language_code": "en",
                    "max_result_count": "20",
                    "min_rating": "4.0",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": '["id"]',
                },
                "destination": {
                    "catalog": "googlemaps",
                    "schema": "places",
                    "table_template": "restaurants_{location_address_slug}",
                }
            }
        },
        # ---------------------------------------------------------------------
        # MATRIX: Generate gas station tables for all German cities
        # Another matrix for a different place type
        # ---------------------------------------------------------------------
        {
            "matrix": {
                "source_table": "places",
                "parameters": generate_city_parameters(
                    GERMAN_CITIES,
                    radius="5000",  # 5km radius for gas stations
                ),
                "common_config": {
                    "included_types": "gas_station",
                    "language_code": "en",
                    "max_result_count": "20",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": '["id"]',
                },
                "destination": {
                    "catalog": "googlemaps",
                    "schema": "places",
                    "table_template": "gas_stations_{location_address_slug}",
                }
            }
        },
        # ---------------------------------------------------------------------
        # MATRIX: Generate EV charging station tables for major cities only
        # You can use a subset of cities for specific use cases
        # ---------------------------------------------------------------------
        {
            "matrix": {
                "source_table": "places",
                "parameters": generate_city_parameters(
                    ["Berlin, Germany", "Munich, Germany", "Hamburg, Germany"],
                    radius="10000",  # Larger radius for EV stations
                ),
                "common_config": {
                    "included_types": "electric_vehicle_charging_station",
                    "language_code": "en",
                    "max_result_count": "20",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": '["id"]',
                },
                "destination": {
                    "catalog": "googlemaps",
                    "schema": "places",
                    "table_template": "ev_stations_{location_address_slug}",
                }
            }
        },
    ],
}

# =============================================================================
# EXPAND AND RUN
# =============================================================================

# Expand matrices to standard pipeline spec
pipeline_spec = expand_pipeline_spec(pipeline_spec_with_matrix)

# Print expansion summary
print("=" * 70)
print("TABLE MATRIX EXPANSION SUMMARY")
print("=" * 70)
print(f"Original objects: {len(pipeline_spec_with_matrix['objects'])}")
print(f"Expanded tables:  {len(pipeline_spec['objects'])}")
print()
print("Generated tables:")
for i, obj in enumerate(pipeline_spec["objects"], 1):
    table = obj["table"]
    dest = table.get("destination_table", table["source_table"])
    source = table["source_table"]
    print(f"  {i:2d}. {dest} (from: {source})")
print("=" * 70)

# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(SOURCE_NAME)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
