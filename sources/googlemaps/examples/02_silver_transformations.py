# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Google Maps - Silver Layer Transformations
# MAGIC 
# MAGIC This notebook transforms the raw Google Maps data by exploding nested JSON structures 
# MAGIC into flattened, queryable tables.
# MAGIC 
# MAGIC ## What This Does
# MAGIC - Explodes `addressComponents` arrays into individual rows
# MAGIC - Flattens nested structs (`location`, `displayName`, `geometry`)
# MAGIC - Creates normalized tables for easier analytics
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC Run `01_get_started.py` first to ingest raw data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION - Update these values to match your environment
# =============================================================================

# Source catalog/schema (where raw data was ingested)
SOURCE_CATALOG = "googlemaps"
SOURCE_SCHEMA = "googlemaps_raw"

# Target catalog/schema (where silver tables will be created)
TARGET_CATALOG = "googlemaps"
TARGET_SCHEMA = "googlemaps_silver"

# Source table names (from your ingestion pipeline)
PLACES_TABLE = "gas_stations_berlin"
GEOCODER_TABLE = "office_coordinates"
DISTANCE_MATRIX_TABLE = "distances_to_gas_stations"

# COMMAND ----------

# Create target schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def table_exists(catalog: str, schema: str, table: str) -> bool:
    """Check if a table exists in Unity Catalog."""
    try:
        spark.table(f"{catalog}.{schema}.{table}")
        return True
    except Exception:
        return False

def table_has_data(catalog: str, schema: str, table: str) -> bool:
    """Check if a table exists and has at least one row."""
    try:
        df = spark.table(f"{catalog}.{schema}.{table}")
        return df.limit(1).count() > 0
    except Exception:
        return False

def safe_write_table(df, target_catalog: str, target_schema: str, table_name: str):
    """Safely write a DataFrame to a table with error handling."""
    try:
        df.write.mode("overwrite").saveAsTable(f"{target_catalog}.{target_schema}.{table_name}")
        print(f"✅ Created: {target_catalog}.{target_schema}.{table_name}")
        return True
    except Exception as e:
        print(f"⚠️ Warning: Could not create {table_name}: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Places - Flatten and Explode

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Places - Core Flattened Table
# MAGIC Flatten the main nested structures from places data

# COMMAND ----------

from pyspark.sql.functions import col, explode, explode_outer, lit, when, size

# Check if places table exists and has data
places_table_ready = table_has_data(SOURCE_CATALOG, SOURCE_SCHEMA, PLACES_TABLE)
places_raw = None

if not places_table_ready:
    print(f"⚠️ Skipping places tables: {PLACES_TABLE} not found or empty")
else:
    # Read places raw data
    places_raw = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{PLACES_TABLE}")
    
    # Flatten places to core fields
    places_flattened = places_raw.select(
        # Primary key
        col("id"),
        
        # Display name (nested struct)
        col("displayName.text").alias("name"),
        col("displayName.languageCode").alias("name_language_code"),
        
        # Address information
        col("formattedAddress"),
        col("shortFormattedAddress"),
        col("adrFormatAddress"),
        
        # Location coordinates (nested struct)
        col("location.latitude"),
        col("location.longitude"),
        
        # Viewport bounds (nested struct)
        col("viewport.low.latitude").alias("viewport_low_lat"),
        col("viewport.low.longitude").alias("viewport_low_lng"),
        col("viewport.high.latitude").alias("viewport_high_lat"),
        col("viewport.high.longitude").alias("viewport_high_lng"),
        
        # Business info
        col("businessStatus"),
        col("priceLevel"),
        col("rating"),
        col("userRatingCount"),
        col("utcOffsetMinutes"),
        
        # Type information
        col("primaryType"),
        col("primaryTypeDisplayName.text").alias("primary_type_display_name"),
        col("types"),  # Keep as array for reference
        
        # Contact info
        col("internationalPhoneNumber"),
        col("nationalPhoneNumber"),
        col("websiteUri"),
        col("googleMapsUri"),
        
        # Editorial
        col("editorialSummary.text").alias("editorial_summary"),
        
        # Plus code (nested struct)
        col("plusCode.globalCode").alias("plus_code_global"),
        col("plusCode.compoundCode").alias("plus_code_compound"),
        
        # Icon info
        col("iconMaskBaseUri"),
        col("iconBackgroundColor"),
        
        # Service attributes (boolean flags)
        col("takeout"),
        col("delivery"),
        col("dineIn"),
        col("reservable"),
        col("servesBreakfast"),
        col("servesLunch"),
        col("servesDinner"),
        col("servesBrunch"),
        col("servesBeer"),
        col("servesWine"),
        col("servesVegetarianFood"),
        col("outdoorSeating"),
        col("liveMusic"),
        col("menuForChildren"),
        col("goodForChildren"),
        col("allowsDogs"),
        col("goodForGroups"),
        col("goodForWatchingSports"),
        
        # Accessibility options (nested struct)
        col("accessibilityOptions.wheelchairAccessibleParking").alias("wheelchair_parking"),
        col("accessibilityOptions.wheelchairAccessibleEntrance").alias("wheelchair_entrance"),
        col("accessibilityOptions.wheelchairAccessibleRestroom").alias("wheelchair_restroom"),
        col("accessibilityOptions.wheelchairAccessibleSeating").alias("wheelchair_seating"),
        
        # Parking options (nested struct)
        col("parkingOptions.freeParking").alias("free_parking"),
        col("parkingOptions.paidParking").alias("paid_parking"),
        col("parkingOptions.freeStreetParking").alias("free_street_parking"),
        col("parkingOptions.paidStreetParking").alias("paid_street_parking"),
        col("parkingOptions.valetParking").alias("valet_parking"),
        col("parkingOptions.freeGarageParking").alias("free_garage_parking"),
        col("parkingOptions.paidGarageParking").alias("paid_garage_parking"),
        
        # Payment options (nested struct)
        col("paymentOptions.acceptsCreditCards").alias("accepts_credit_cards"),
        col("paymentOptions.acceptsDebitCards").alias("accepts_debit_cards"),
        col("paymentOptions.acceptsCashOnly").alias("accepts_cash_only"),
        col("paymentOptions.acceptsNfc").alias("accepts_nfc"),
        
        # Opening hours - current (nested struct)
        col("currentOpeningHours.openNow").alias("currently_open"),
        col("currentOpeningHours.weekdayDescriptions").alias("current_weekday_descriptions"),
        
        # Opening hours - regular (nested struct)
        col("regularOpeningHours.openNow").alias("regular_open_now"),
        col("regularOpeningHours.weekdayDescriptions").alias("regular_weekday_descriptions"),
    )
    
    safe_write_table(places_flattened, TARGET_CATALOG, TARGET_SCHEMA, "places_flattened")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Places - Address Components (Exploded)
# MAGIC One row per address component (street, city, country, etc.)

# COMMAND ----------

if places_raw is not None:
    # Explode address components array
    places_address_components = places_raw.select(
        col("id").alias("place_id"),
        col("displayName.text").alias("place_name"),
        explode_outer(col("addressComponents")).alias("component")
    ).select(
        col("place_id"),
        col("place_name"),
        col("component.longText").alias("long_text"),
        col("component.shortText").alias("short_text"),
        col("component.types").alias("component_types"),
        col("component.languageCode").alias("language_code"),
    )
    
    safe_write_table(places_address_components, TARGET_CATALOG, TARGET_SCHEMA, "places_address_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Places - Types (Exploded)
# MAGIC One row per place type

# COMMAND ----------

if places_raw is not None:
    # Explode types array
    places_types = places_raw.select(
        col("id").alias("place_id"),
        col("displayName.text").alias("place_name"),
        explode_outer(col("types")).alias("place_type")
    )
    
    safe_write_table(places_types, TARGET_CATALOG, TARGET_SCHEMA, "places_types")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Places - Opening Hours Periods (Exploded)
# MAGIC One row per opening/closing period

# COMMAND ----------

if places_raw is not None:
    # Explode regular opening hours periods
    places_opening_hours = places_raw.select(
        col("id").alias("place_id"),
        col("displayName.text").alias("place_name"),
        explode_outer(col("regularOpeningHours.periods")).alias("period")
    ).select(
        col("place_id"),
        col("place_name"),
        col("period.open.day").alias("open_day"),
        col("period.open.hour").alias("open_hour"),
        col("period.open.minute").alias("open_minute"),
        col("period.close.day").alias("close_day"),
        col("period.close.hour").alias("close_hour"),
        col("period.close.minute").alias("close_minute"),
    )
    
    safe_write_table(places_opening_hours, TARGET_CATALOG, TARGET_SCHEMA, "places_opening_hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Geocoder - Flatten and Explode

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Geocoder - Core Flattened Table

# COMMAND ----------

# Check if geocoder table exists and has data
geocoder_table_ready = table_has_data(SOURCE_CATALOG, SOURCE_SCHEMA, GEOCODER_TABLE)

if geocoder_table_ready:
    # Read geocoder raw data
    geocoder_raw = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{GEOCODER_TABLE}")
    
    # Flatten geocoder to core fields
    geocoder_flattened = geocoder_raw.select(
        # Primary key
        col("place_id"),
        
        # Address
        col("formatted_address"),
        col("types"),  # Keep as array for reference
        col("partial_match"),
        
        # Location coordinates (nested geometry struct)
        col("geometry.location.lat").alias("latitude"),
        col("geometry.location.lng").alias("longitude"),
        col("geometry.location_type").alias("location_type"),
        
        # Viewport bounds
        col("geometry.viewport.northeast.lat").alias("viewport_ne_lat"),
        col("geometry.viewport.northeast.lng").alias("viewport_ne_lng"),
        col("geometry.viewport.southwest.lat").alias("viewport_sw_lat"),
        col("geometry.viewport.southwest.lng").alias("viewport_sw_lng"),
        
        # Bounds (if available)
        col("geometry.bounds.northeast.lat").alias("bounds_ne_lat"),
        col("geometry.bounds.northeast.lng").alias("bounds_ne_lng"),
        col("geometry.bounds.southwest.lat").alias("bounds_sw_lat"),
        col("geometry.bounds.southwest.lng").alias("bounds_sw_lng"),
        
        # Plus code
        col("plus_code.global_code").alias("plus_code_global"),
        col("plus_code.compound_code").alias("plus_code_compound"),
        
        # Postcode localities (array)
        col("postcode_localities"),
    )
    
    safe_write_table(geocoder_flattened, TARGET_CATALOG, TARGET_SCHEMA, "geocoder_flattened")
else:
    print(f"⚠️ Skipping geocoder tables: {GEOCODER_TABLE} not found or empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Geocoder - Address Components (Exploded)
# MAGIC One row per address component

# COMMAND ----------

if geocoder_table_ready:
    # Explode address components array
    geocoder_address_components = geocoder_raw.select(
        col("place_id"),
        col("formatted_address"),
        explode_outer(col("address_components")).alias("component")
    ).select(
        col("place_id"),
        col("formatted_address"),
        col("component.long_name"),
        col("component.short_name"),
        col("component.types").alias("component_types"),
    )
    
    safe_write_table(geocoder_address_components, TARGET_CATALOG, TARGET_SCHEMA, "geocoder_address_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Geocoder - Address Components Pivoted
# MAGIC Extract common address components as columns

# COMMAND ----------

from pyspark.sql.functions import first, array_contains

if geocoder_table_ready:
    try:
        # Start with base geocoder info - cache to avoid repeated reads
        geocoder_base = geocoder_raw.select(
            col("place_id"),
            col("formatted_address"),
            col("geometry.location.lat").alias("latitude"),
            col("geometry.location.lng").alias("longitude"),
            col("address_components"),
        ).cache()
        
        # Explode address components once and cache
        geocoder_exploded = geocoder_base.select(
            col("place_id"),
            explode_outer(col("address_components")).alias("component")
        ).cache()
        
        # Start with base info (without address_components)
        geocoder_address_parsed = geocoder_base.select(
            col("place_id"),
            col("formatted_address"),
            col("latitude"),
            col("longitude"),
        )
        
        # Define component types to extract
        component_mappings = [
            ("street_number", "street_number"),
            ("route", "street_name"),
            ("locality", "city"),
            ("administrative_area_level_1", "state_province"),
            ("administrative_area_level_2", "county"),
            ("country", "country"),
            ("postal_code", "postal_code"),
            ("sublocality", "sublocality"),
            ("neighborhood", "neighborhood"),
        ]
        
        # Extract each component type
        for component_type, column_name in component_mappings:
            component_df = geocoder_exploded.filter(
                array_contains(col("component.types"), component_type)
            ).select(
                col("place_id"),
                col("component.long_name").alias(f"{column_name}_long"),
                col("component.short_name").alias(f"{column_name}_short"),
            ).dropDuplicates(["place_id"])
            
            geocoder_address_parsed = geocoder_address_parsed.join(
                component_df, on="place_id", how="left"
            )
        
        # Clean up cache
        geocoder_base.unpersist()
        geocoder_exploded.unpersist()
        
        safe_write_table(geocoder_address_parsed, TARGET_CATALOG, TARGET_SCHEMA, "geocoder_address_parsed")
        
    except Exception as e:
        print(f"⚠️ Warning: Could not create geocoder_address_parsed: {str(e)}")
        # Try a simpler version without pivoting
        try:
            geocoder_address_simple = geocoder_raw.select(
                col("place_id"),
                col("formatted_address"),
                col("geometry.location.lat").alias("latitude"),
                col("geometry.location.lng").alias("longitude"),
            )
            safe_write_table(geocoder_address_simple, TARGET_CATALOG, TARGET_SCHEMA, "geocoder_address_parsed")
        except Exception as e2:
            print(f"⚠️ Warning: Fallback also failed: {str(e2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Distance Matrix - Flatten

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Distance Matrix - Core Flattened Table
# MAGIC 
# MAGIC Flatten nested structs (distance, duration, fare) into individual columns.

# COMMAND ----------

# Check if distance matrix table exists and has data
distance_matrix_table_ready = table_has_data(SOURCE_CATALOG, SOURCE_SCHEMA, DISTANCE_MATRIX_TABLE)

if not distance_matrix_table_ready:
    print(f"⚠️ Skipping distance matrix tables: {DISTANCE_MATRIX_TABLE} not found or empty")
else:
    # Read distance matrix raw data
    distance_matrix_raw = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{DISTANCE_MATRIX_TABLE}")
    
    # Flatten distance matrix - access nested struct fields directly
    # Schema: distance STRUCT<value: BIGINT, text: STRING>
    distance_matrix_flattened = distance_matrix_raw.select(
        # Keys
        col("origin_index"),
        col("destination_index"),
        
        # Addresses
        col("origin_address"),
        col("destination_address"),
        
        # Status
        col("status"),
        
        # Distance (nested struct with value: BIGINT, text: STRING)
        col("distance.value").alias("distance_meters"),
        col("distance.text").alias("distance_text"),
        
        # Duration (nested struct)
        col("duration.value").alias("duration_seconds"),
        col("duration.text").alias("duration_text"),
        
        # Duration in traffic (nested struct, may be null)
        col("duration_in_traffic.value").alias("duration_in_traffic_seconds"),
        col("duration_in_traffic.text").alias("duration_in_traffic_text"),
        
        # Fare (nested struct, transit mode only, may be null)
        col("fare.currency").alias("fare_currency"),
        col("fare.value").alias("fare_value"),
        col("fare.text").alias("fare_text"),
    )
    
    # Add calculated fields
    distance_matrix_flattened = distance_matrix_flattened.withColumn(
        "distance_km", 
        col("distance_meters") / 1000.0
    ).withColumn(
        "duration_minutes",
        col("duration_seconds") / 60.0
    ).withColumn(
        "duration_in_traffic_minutes",
        when(col("duration_in_traffic_seconds").isNotNull(), 
             col("duration_in_traffic_seconds") / 60.0)
    ).withColumn(
        "traffic_delay_minutes",
        when(col("duration_in_traffic_seconds").isNotNull(),
             (col("duration_in_traffic_seconds") - col("duration_seconds")) / 60.0)
    )
    
    safe_write_table(distance_matrix_flattened, TARGET_CATALOG, TARGET_SCHEMA, "distance_matrix_flattened")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Show all created tables
print("=" * 60)
print("SILVER LAYER TABLES CREATED")
print("=" * 60)

try:
    silver_tables = spark.sql(f"SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA}").collect()
    
    if silver_tables:
        for table in silver_tables:
            table_name = table["tableName"]
            try:
                count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}").count()
                print(f"📊 {table_name}: {count:,} rows")
            except Exception as e:
                print(f"⚠️ {table_name}: Could not read ({str(e)[:50]}...)")
    else:
        print("No tables created in silver layer.")
except Exception as e:
    print(f"⚠️ Could not list tables: {str(e)}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference: Silver Tables Created
# MAGIC 
# MAGIC ### Places Tables
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `places_flattened` | Core place data with all nested structs flattened |
# MAGIC | `places_address_components` | One row per address component (city, street, etc.) |
# MAGIC | `places_types` | One row per place type |
# MAGIC | `places_opening_hours` | One row per opening/closing period |
# MAGIC 
# MAGIC ### Geocoder Tables
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `geocoder_flattened` | Core geocoding data with nested structs flattened |
# MAGIC | `geocoder_address_components` | One row per address component |
# MAGIC | `geocoder_address_parsed` | Address components pivoted into columns |
# MAGIC 
# MAGIC ### Distance Matrix Tables
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `distance_matrix_flattened` | Distance/duration data with calculated fields |

