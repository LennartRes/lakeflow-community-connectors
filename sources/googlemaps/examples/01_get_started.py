# Databricks notebook source
# MAGIC %md
# MAGIC # 🗺️ Google Maps Connector - Get Started
# MAGIC 
# MAGIC This notebook demonstrates how to ingest data from Google Maps APIs into your Lakehouse.
# MAGIC 
# MAGIC **Supported Tables:**
# MAGIC - `places` - Location data from Google's database of 200M+ places
# MAGIC - `geocoder` - Address-to-coordinates and reverse geocoding
# MAGIC - `distance_matrix` - Travel distance and time calculations
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 1. Google Cloud Project with billing enabled
# MAGIC 2. APIs enabled: Places API (New), Geocoding API, Distance Matrix API
# MAGIC 3. API Key from Google Cloud Console → APIs & Services → Credentials

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC Update these values to match your environment

# COMMAND ----------

# =============================================================================
# CONFIGURATION - Update these values
# =============================================================================

# Your Unity Catalog connection name (configured with your Google Maps API key)
CONNECTION_NAME = "googlemaps"

# Destination catalog and schema for your ingested data
# NOTE: Ensure this catalog exists in your Unity Catalog before running
DESTINATION_CATALOG = "googlemaps"
DESTINATION_SCHEMA = "googlemaps_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Your Ingestion Pipeline
# MAGIC 
# MAGIC Choose from the examples below and customize for your use case.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example A: Places Search
# MAGIC Search for places using natural language queries

# COMMAND ----------

# Places API - Find businesses, landmarks, or points of interest
places_example = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        {
            "table": {
                "source_table": "places",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "restaurants_berlin",
                "table_configuration": {
                    # REQUIRED: Natural language search query
                    "text_query": "Italian restaurants in Berlin",
                    
                    # OPTIONAL: Customize your search
                    "language_code": "en",          # Language for results
                    "max_result_count": "20",       # Results per page (1-20)
                    "included_type": "restaurant",  # Restrict to place type
                    "min_rating": "4.0",            # Minimum rating filter
                    "open_now": "true",             # Only open places
                    "region_code": "DE",            # Region bias
                    
                    # SCD Type: SCD_TYPE_1 (upsert), SCD_TYPE_2 (history), APPEND_ONLY
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["id"],
                },
            }
        },
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example B: Geocoding
# MAGIC Convert addresses to coordinates or coordinates to addresses

# COMMAND ----------

# Geocoding API - Address to coordinates (forward geocoding)
geocoder_forward_example = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        {
            "table": {
                "source_table": "geocoder",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "office_locations",
                "table_configuration": {
                    # Forward geocoding: address to coordinates
                    "address": "Köpenicker Str. 41, 10179 Berlin",
                    
                    # OPTIONAL parameters
                    "language": "en",
                    "region": "de",  # Region bias (ISO 3166-1 country code)
                    
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}

# Geocoding API - Coordinates to address (reverse geocoding)
geocoder_reverse_example = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        {
            "table": {
                "source_table": "geocoder",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "reverse_geocoded",
                "table_configuration": {
                    # Reverse geocoding: coordinates to address
                    "latlng": "52.5200,13.4050",  # Berlin coordinates
                    "language": "en",
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example C: Distance Matrix
# MAGIC Calculate travel times and distances between multiple locations

# COMMAND ----------

# Distance Matrix API - Calculate distances between origins and destinations
distance_matrix_example = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        {
            "table": {
                "source_table": "distance_matrix",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "travel_distances",
                "table_configuration": {
                    # REQUIRED: Origins and destinations (pipe-separated for multiple)
                    "origins": "Berlin, Germany|Munich, Germany",
                    "destinations": "Frankfurt, Germany|Hamburg, Germany",
                    
                    # OPTIONAL: Travel mode (driving, walking, bicycling, transit)
                    "mode": "driving",
                    
                    # OPTIONAL: Traffic information
                    "departure_time": "now",  # or Unix timestamp
                    "traffic_model": "best_guess",  # best_guess, pessimistic, optimistic
                    
                    # OPTIONAL: Units and language
                    "units": "metric",  # metric or imperial
                    "language": "en",
                    
                    # OPTIONAL: Avoid certain route features
                    # "avoid": "tolls|highways",  # tolls, highways, ferries, indoor
                    
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example D: Combined Pipeline
# MAGIC Ingest multiple tables in a single pipeline run

# COMMAND ----------

# Combined pipeline - Ingest multiple tables together
combined_pipeline_spec = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        # Places: Search for gas stations
        {
            "table": {
                "source_table": "places",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "gas_stations_berlin",
                "table_configuration": {
                    "text_query": "Gas stations in Berlin",
                    "included_type": "gas_station",
                    "max_result_count": "20",
                    "region_code": "DE",
                    "scd_type": "SCD_TYPE_1",
                    "primary_keys": ["id"],
                },
            }
        },
        # Geocoder: Get coordinates for office
        {
            "table": {
                "source_table": "geocoder",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "office_coordinates",
                "table_configuration": {
                    "address": "Köpenicker Str. 41, 10179 Berlin",
                    "language": "en",
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
        # Distance Matrix: Calculate distances from office to gas stations
        {
            "table": {
                "source_table": "distance_matrix",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": "distances_to_gas_stations",
                "table_configuration": {
                    "origins": "Köpenicker Str. 41, 10179 Berlin",
                    "destinations": "Alexanderplatz, Berlin|Potsdamer Platz, Berlin",
                    "mode": "driving",
                    "units": "metric",
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run the Ingestion Pipeline
# MAGIC 
# MAGIC Select which pipeline spec to run by uncommenting the appropriate line.

# COMMAND ----------

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Register the Google Maps source
source_name = "googlemaps"
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# COMMAND ----------

# Choose your pipeline spec (uncomment one):
pipeline_spec = combined_pipeline_spec      # Full combined example
# pipeline_spec = places_example            # Just places
# pipeline_spec = geocoder_forward_example  # Just forward geocoding
# pipeline_spec = geocoder_reverse_example  # Just reverse geocoding
# pipeline_spec = distance_matrix_example   # Just distance matrix

# Run the ingestion
ingest(spark, pipeline_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Query Your Data
# MAGIC 
# MAGIC After running the pipeline, query your ingested data:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View places data (uncomment to query)
# MAGIC -- SELECT * FROM googlemaps.googlemaps_raw.gas_stations_berlin LIMIT 10;
# MAGIC 
# MAGIC -- View geocoder data
# MAGIC -- SELECT * FROM googlemaps.googlemaps_raw.office_coordinates LIMIT 10;
# MAGIC 
# MAGIC -- View distance matrix data
# MAGIC -- SELECT * FROM googlemaps.googlemaps_raw.distances_to_gas_stations LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Reference
# MAGIC 
# MAGIC ### Places Table Options
# MAGIC | Option | Required | Description |
# MAGIC |--------|----------|-------------|
# MAGIC | `text_query` | Yes | Natural language search query |
# MAGIC | `language_code` | No | Language for results (e.g., "en") |
# MAGIC | `max_result_count` | No | Results per page (1-20, default 20) |
# MAGIC | `included_type` | No | Restrict to place type (e.g., "restaurant") |
# MAGIC | `min_rating` | No | Minimum rating filter (1.0-5.0) |
# MAGIC | `open_now` | No | Only return open places ("true"/"false") |
# MAGIC | `region_code` | No | Region code for biasing (e.g., "US") |
# MAGIC 
# MAGIC ### Geocoder Table Options
# MAGIC | Option | Required | Description |
# MAGIC |--------|----------|-------------|
# MAGIC | `address` | Yes* | Address to geocode |
# MAGIC | `latlng` | Yes* | Coordinates for reverse geocoding (format: "lat,lng") |
# MAGIC | `place_id` | Yes* | Place ID to geocode |
# MAGIC | `language` | No | Language for results |
# MAGIC | `region` | No | Region bias |
# MAGIC 
# MAGIC *One of address, latlng, or place_id is required
# MAGIC 
# MAGIC ### Distance Matrix Table Options
# MAGIC | Option | Required | Description |
# MAGIC |--------|----------|-------------|
# MAGIC | `origins` | Yes | Pipe-separated origin locations (e.g., "Berlin\|Munich") |
# MAGIC | `destinations` | Yes | Pipe-separated destination locations |
# MAGIC | `mode` | No | Travel mode: driving, walking, bicycling, transit |
# MAGIC | `language` | No | Language code for results (e.g., "en") |
# MAGIC | `departure_time` | No | Unix timestamp or "now" |
# MAGIC | `traffic_model` | No | best_guess, pessimistic, optimistic |
# MAGIC | `units` | No | metric or imperial |
# MAGIC | `avoid` | No | Features to avoid: tolls\|highways\|ferries\|indoor |
# MAGIC 
# MAGIC ### SCD Types
# MAGIC | Type | Description |
# MAGIC |------|-------------|
# MAGIC | `SCD_TYPE_1` | Overwrite existing records (default) |
# MAGIC | `SCD_TYPE_2` | Keep full history with versioning |
# MAGIC | `APPEND_ONLY` | Only add new records, never update |

