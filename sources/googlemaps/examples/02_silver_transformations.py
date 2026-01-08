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
# MAGIC ## 1. Places - Flatten and Explode

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Places - Core Flattened Table
# MAGIC Flatten the main nested structures from places data

# COMMAND ----------

from pyspark.sql.functions import col, explode, explode_outer, lit, when, size

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

# Write flattened places table
places_flattened.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.places_flattened"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.places_flattened")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Places - Address Components (Exploded)
# MAGIC One row per address component (street, city, country, etc.)

# COMMAND ----------

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

# Write address components table
places_address_components.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.places_address_components"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.places_address_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Places - Types (Exploded)
# MAGIC One row per place type

# COMMAND ----------

# Explode types array
places_types = places_raw.select(
    col("id").alias("place_id"),
    col("displayName.text").alias("place_name"),
    explode_outer(col("types")).alias("place_type")
)

# Write types table
places_types.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.places_types"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.places_types")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Places - Opening Hours Periods (Exploded)
# MAGIC One row per opening/closing period

# COMMAND ----------

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

# Write opening hours table
places_opening_hours.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.places_opening_hours"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.places_opening_hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Geocoder - Flatten and Explode

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Geocoder - Core Flattened Table

# COMMAND ----------

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

# Write flattened geocoder table
geocoder_flattened.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_flattened"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_flattened")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Geocoder - Address Components (Exploded)
# MAGIC One row per address component

# COMMAND ----------

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

# Write address components table
geocoder_address_components.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_address_components"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_address_components")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Geocoder - Address Components Pivoted
# MAGIC Extract common address components as columns

# COMMAND ----------

from pyspark.sql.functions import first, array_contains

# Pivot common address components
geocoder_pivot = geocoder_raw.select(
    col("place_id"),
    col("formatted_address"),
    explode_outer(col("address_components")).alias("component")
)

# Extract specific component types
geocoder_address_parsed = geocoder_raw.select(
    col("place_id"),
    col("formatted_address"),
    col("geometry.location.lat").alias("latitude"),
    col("geometry.location.lng").alias("longitude"),
).alias("base")

# Join with exploded components and pivot
for component_type, column_name in [
    ("street_number", "street_number"),
    ("route", "street_name"),
    ("locality", "city"),
    ("administrative_area_level_1", "state_province"),
    ("administrative_area_level_2", "county"),
    ("country", "country"),
    ("postal_code", "postal_code"),
    ("sublocality", "sublocality"),
    ("neighborhood", "neighborhood"),
]:
    component_df = geocoder_raw.select(
        col("place_id"),
        explode_outer(col("address_components")).alias("component")
    ).filter(
        array_contains(col("component.types"), component_type)
    ).select(
        col("place_id"),
        col("component.long_name").alias(f"{column_name}_long"),
        col("component.short_name").alias(f"{column_name}_short"),
    ).dropDuplicates(["place_id"])
    
    geocoder_address_parsed = geocoder_address_parsed.join(
        component_df, on="place_id", how="left"
    )

# Write parsed address table
geocoder_address_parsed.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_address_parsed"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.geocoder_address_parsed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Distance Matrix - Flatten

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Distance Matrix - Core Flattened Table

# COMMAND ----------

# Read distance matrix raw data
distance_matrix_raw = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{DISTANCE_MATRIX_TABLE}")

# Flatten distance matrix
distance_matrix_flattened = distance_matrix_raw.select(
    # Keys
    col("origin_index"),
    col("destination_index"),
    
    # Addresses
    col("origin_address"),
    col("destination_address"),
    
    # Status
    col("status"),
    
    # Distance (nested struct)
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

# Write flattened distance matrix table
distance_matrix_flattened.write.mode("overwrite").saveAsTable(
    f"{TARGET_CATALOG}.{TARGET_SCHEMA}.distance_matrix_flattened"
)

print(f"✅ Created: {TARGET_CATALOG}.{TARGET_SCHEMA}.distance_matrix_flattened")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Show all created tables
print("=" * 60)
print("SILVER LAYER TABLES CREATED")
print("=" * 60)

silver_tables = spark.sql(f"SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA}").collect()

for table in silver_tables:
    table_name = table["tableName"]
    count = spark.table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}").count()
    print(f"📊 {table_name}: {count:,} rows")

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

