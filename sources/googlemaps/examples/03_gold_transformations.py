# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Google Maps - Gold Layer Transformations
# MAGIC 
# MAGIC This notebook creates analytics-ready gold tables by joining places, geocoder, 
# MAGIC and distance matrix data with calculated columns and real value outputs.
# MAGIC 
# MAGIC ## Use Cases
# MAGIC - **Location Intelligence**: Combine place ratings, locations, and distances
# MAGIC - **Site Analysis**: Find optimal locations based on multiple criteria
# MAGIC - **Travel Planning**: Calculate routes with traffic and cost analysis
# MAGIC - **Competitive Analysis**: Analyze business density and ratings by area
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 1. Run `01_get_started.py` to ingest raw data
# MAGIC 2. Run `02_silver_transformations.py` to create silver layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION - Update these values to match your environment
# =============================================================================

# Silver layer catalog/schema (where silver tables are located)
SILVER_CATALOG = "googlemaps"
SILVER_SCHEMA = "googlemaps_silver"

# Gold layer catalog/schema (where gold tables will be created)
GOLD_CATALOG = "googlemaps"
GOLD_SCHEMA = "googlemaps_gold"

# Reference point for distance calculations (e.g., your office)
REFERENCE_LOCATION = {
    "name": "Headquarters",
    "address": "Köpenicker Str. 41, 10179 Berlin",
    "latitude": 52.5095,
    "longitude": 13.4282,
}

# COMMAND ----------

# Create target schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, round as spark_round,
    expr, row_number, rank, dense_rank, percent_rank,
    avg, sum as spark_sum, count, min as spark_min, max as spark_max,
    sqrt, pow, sin, cos, atan2, radians, degrees,
    current_timestamp, date_format,
    array_contains, size, first, collect_list,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Location Intelligence Table
# MAGIC 
# MAGIC Comprehensive location data combining places with geocoder enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Haversine Distance UDF
# MAGIC Calculate distance between two coordinates in kilometers

# COMMAND ----------

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees) using Spark SQL functions.
    Returns distance in kilometers.
    """
    # Convert to radians
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    lon1_rad = radians(lon1)
    lon2_rad = radians(lon2)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = pow(sin(dlat / 2), 2) + cos(lat1_rad) * cos(lat2_rad) * pow(sin(dlon / 2), 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    # Earth's radius in kilometers
    earth_radius_km = 6371.0
    
    return c * earth_radius_km

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Places with Location Intelligence

# COMMAND ----------

# Read places flattened data
places = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.places_flattened")

# Add distance from reference point
places_with_distance = places.withColumn(
    "distance_from_reference_km",
    spark_round(
        haversine_distance(
            col("latitude"), col("longitude"),
            lit(REFERENCE_LOCATION["latitude"]), lit(REFERENCE_LOCATION["longitude"])
        ), 2
    )
).withColumn(
    "reference_location", 
    lit(REFERENCE_LOCATION["name"])
)

# Add rating score (weighted by number of reviews)
places_with_scores = places_with_distance.withColumn(
    "rating_confidence",
    when(col("userRatingCount") >= 100, "high")
    .when(col("userRatingCount") >= 20, "medium")
    .otherwise("low")
).withColumn(
    # Bayesian average rating (weighted towards mean)
    "weighted_rating",
    spark_round(
        (col("rating") * col("userRatingCount") + 3.5 * 10) / 
        (col("userRatingCount") + 10),
        2
    )
)

# Add service level scoring
places_with_scores = places_with_scores.withColumn(
    "service_score",
    (
        when(col("takeout") == True, 1).otherwise(0) +
        when(col("delivery") == True, 1).otherwise(0) +
        when(col("dineIn") == True, 1).otherwise(0) +
        when(col("reservable") == True, 1).otherwise(0) +
        when(col("outdoorSeating") == True, 1).otherwise(0)
    )
).withColumn(
    "accessibility_score",
    (
        when(col("wheelchair_parking") == True, 1).otherwise(0) +
        when(col("wheelchair_entrance") == True, 1).otherwise(0) +
        when(col("wheelchair_restroom") == True, 1).otherwise(0) +
        when(col("wheelchair_seating") == True, 1).otherwise(0)
    )
)

# Add ranking within dataset
window_by_rating = Window.orderBy(col("weighted_rating").desc())
window_by_distance = Window.orderBy(col("distance_from_reference_km").asc())

places_ranked = places_with_scores.withColumn(
    "rating_rank", dense_rank().over(window_by_rating)
).withColumn(
    "distance_rank", dense_rank().over(window_by_distance)
).withColumn(
    # Combined score: closer + higher rated = better
    "overall_score",
    spark_round(
        (col("weighted_rating") * 10) / (1 + col("distance_from_reference_km") * 0.1),
        2
    )
)

# Final gold table
places_intelligence = places_ranked.select(
    # Identity
    col("id").alias("place_id"),
    col("name"),
    col("formattedAddress").alias("address"),
    
    # Location
    col("latitude"),
    col("longitude"),
    col("distance_from_reference_km"),
    col("reference_location"),
    col("distance_rank"),
    
    # Rating & Reviews
    col("rating"),
    col("userRatingCount").alias("review_count"),
    col("weighted_rating"),
    col("rating_confidence"),
    col("rating_rank"),
    
    # Business Info
    col("businessStatus").alias("business_status"),
    col("priceLevel").alias("price_level"),
    col("primaryType").alias("place_type"),
    col("currently_open").alias("is_open_now"),
    
    # Contact
    col("internationalPhoneNumber").alias("phone"),
    col("websiteUri").alias("website"),
    col("googleMapsUri").alias("google_maps_url"),
    
    # Scores
    col("service_score"),
    col("accessibility_score"),
    col("overall_score"),
    
    # Service flags
    col("takeout"),
    col("delivery"),
    col("dineIn").alias("dine_in"),
    col("reservable"),
    
    # Metadata
    current_timestamp().alias("processed_at"),
)

# Write gold table
places_intelligence.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.places_intelligence"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.places_intelligence")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Travel Analysis Table
# MAGIC 
# MAGIC Combine distance matrix data with places for route optimization

# COMMAND ----------

# Read distance matrix flattened data
distance_matrix = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.distance_matrix_flattened")

# Create travel analysis with calculated fields
travel_analysis = distance_matrix.select(
    # Route identification
    col("origin_address"),
    col("destination_address"),
    col("origin_index"),
    col("destination_index"),
    
    # Status
    col("status").alias("route_status"),
    
    # Distance
    col("distance_meters"),
    col("distance_km"),
    col("distance_text"),
    
    # Duration
    col("duration_seconds"),
    col("duration_minutes"),
    col("duration_text"),
    
    # Traffic
    col("duration_in_traffic_seconds"),
    col("duration_in_traffic_minutes"),
    col("duration_in_traffic_text"),
    col("traffic_delay_minutes"),
    
    # Fare (if transit)
    col("fare_currency"),
    col("fare_value"),
    col("fare_text"),
).withColumn(
    # Calculate average speed
    "average_speed_kmh",
    when(col("duration_seconds") > 0,
         spark_round(col("distance_km") / (col("duration_seconds") / 3600), 1))
).withColumn(
    # Traffic impact percentage
    "traffic_delay_percent",
    when(col("duration_in_traffic_seconds").isNotNull() & (col("duration_seconds") > 0),
         spark_round(
             ((col("duration_in_traffic_seconds") - col("duration_seconds")) / 
              col("duration_seconds")) * 100, 1
         ))
).withColumn(
    # Route efficiency category
    "route_category",
    when(col("distance_km") < 5, "short")
    .when(col("distance_km") < 20, "medium")
    .when(col("distance_km") < 50, "long")
    .otherwise("very_long")
).withColumn(
    # Travel time category
    "duration_category",
    when(col("duration_minutes") < 10, "quick")
    .when(col("duration_minutes") < 30, "moderate")
    .when(col("duration_minutes") < 60, "long")
    .otherwise("very_long")
).withColumn(
    # Estimated fuel cost (assuming 8L/100km @ €1.60/L)
    "estimated_fuel_cost_eur",
    spark_round(col("distance_km") * 0.08 * 1.60, 2)
).withColumn(
    "processed_at",
    current_timestamp()
)

# Write gold table
travel_analysis.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.travel_analysis"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.travel_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Location Summary Statistics
# MAGIC 
# MAGIC Aggregate statistics for business intelligence

# COMMAND ----------

# Read places intelligence for aggregations
places_intel = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.places_intelligence")

# Overall summary statistics
location_summary = places_intel.agg(
    count("*").alias("total_places"),
    spark_round(avg("rating"), 2).alias("avg_rating"),
    spark_round(avg("weighted_rating"), 2).alias("avg_weighted_rating"),
    spark_sum("review_count").cast("long").alias("total_reviews"),
    spark_round(avg("distance_from_reference_km"), 2).alias("avg_distance_km"),
    spark_round(spark_min("distance_from_reference_km"), 2).alias("min_distance_km"),
    spark_round(spark_max("distance_from_reference_km"), 2).alias("max_distance_km"),
    spark_round(avg("service_score"), 2).alias("avg_service_score"),
    spark_round(avg("accessibility_score"), 2).alias("avg_accessibility_score"),
    spark_round(avg("overall_score"), 2).alias("avg_overall_score"),
    count(when(col("is_open_now") == True, 1)).alias("currently_open_count"),
    count(when(col("takeout") == True, 1)).alias("offers_takeout_count"),
    count(when(col("delivery") == True, 1)).alias("offers_delivery_count"),
).withColumn(
    "reference_location", lit(REFERENCE_LOCATION["name"])
).withColumn(
    "processed_at", current_timestamp()
)

# Write summary table
location_summary.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.location_summary"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.location_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Places by Type Analysis
# MAGIC 
# MAGIC Breakdown of places by type with statistics

# COMMAND ----------

# Aggregate by place type
places_by_type = places_intel.groupBy("place_type").agg(
    count("*").alias("place_count"),
    spark_round(avg("rating"), 2).alias("avg_rating"),
    spark_round(avg("weighted_rating"), 2).alias("avg_weighted_rating"),
    spark_sum("review_count").cast("long").alias("total_reviews"),
    spark_round(avg("distance_from_reference_km"), 2).alias("avg_distance_km"),
    spark_round(avg("service_score"), 2).alias("avg_service_score"),
    spark_round(avg("accessibility_score"), 2).alias("avg_accessibility_score"),
    count(when(col("is_open_now") == True, 1)).alias("currently_open"),
    spark_round(spark_min("rating"), 2).alias("min_rating"),
    spark_round(spark_max("rating"), 2).alias("max_rating"),
).orderBy(col("place_count").desc()).withColumn(
    "processed_at", current_timestamp()
)

# Write places by type table
places_by_type.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.places_by_type"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.places_by_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top Rated Places View
# MAGIC 
# MAGIC Best places by weighted rating with minimum review threshold

# COMMAND ----------

# Filter for places with sufficient reviews and high ratings
top_rated = places_intel.filter(
    (col("review_count") >= 10) & (col("rating").isNotNull())
).orderBy(
    col("weighted_rating").desc()
).limit(50).select(
    "place_id",
    "name",
    "address",
    "latitude",
    "longitude",
    "rating",
    "review_count",
    "weighted_rating",
    "rating_confidence",
    "place_type",
    "price_level",
    "distance_from_reference_km",
    "phone",
    "website",
    "google_maps_url",
    "is_open_now",
    "takeout",
    "delivery",
    "dine_in",
    current_timestamp().alias("processed_at"),
)

# Write top rated table
top_rated.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.top_rated_places"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.top_rated_places")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Nearest Places View
# MAGIC 
# MAGIC Closest places to reference location

# COMMAND ----------

# Get nearest places
nearest_places = places_intel.orderBy(
    col("distance_from_reference_km").asc()
).limit(50).select(
    "place_id",
    "name",
    "address",
    "latitude",
    "longitude",
    "distance_from_reference_km",
    "reference_location",
    "rating",
    "weighted_rating",
    "review_count",
    "place_type",
    "is_open_now",
    "phone",
    "google_maps_url",
    current_timestamp().alias("processed_at"),
)

# Write nearest places table
nearest_places.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.nearest_places"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.nearest_places")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Geocoder Enriched Locations
# MAGIC 
# MAGIC Parsed and enriched geocoder data

# COMMAND ----------

# Read geocoder data
geocoder_parsed = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.geocoder_address_parsed")
geocoder_flat = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.geocoder_flattened")

# Create enriched locations table
geocoder_enriched = geocoder_flat.join(
    geocoder_parsed.select(
        "place_id",
        "street_number_long",
        "street_name_long",
        "city_long",
        "state_province_long",
        "country_long",
        "postal_code_long",
    ),
    on="place_id",
    how="left"
).withColumn(
    # Distance from reference
    "distance_from_reference_km",
    spark_round(
        haversine_distance(
            col("latitude"), col("longitude"),
            lit(REFERENCE_LOCATION["latitude"]), lit(REFERENCE_LOCATION["longitude"])
        ), 2
    )
).withColumn(
    "reference_location",
    lit(REFERENCE_LOCATION["name"])
).withColumn(
    # Full street address
    "street_address",
    concat(
        coalesce(col("street_number_long"), lit("")),
        lit(" "),
        coalesce(col("street_name_long"), lit(""))
    )
).select(
    col("place_id"),
    col("formatted_address"),
    col("street_address"),
    col("city_long").alias("city"),
    col("state_province_long").alias("state_province"),
    col("country_long").alias("country"),
    col("postal_code_long").alias("postal_code"),
    col("latitude"),
    col("longitude"),
    col("location_type"),
    col("plus_code_global"),
    col("plus_code_compound"),
    col("distance_from_reference_km"),
    col("reference_location"),
    col("partial_match"),
    current_timestamp().alias("processed_at"),
)

# Write enriched geocoder table
geocoder_enriched.write.mode("overwrite").saveAsTable(
    f"{GOLD_CATALOG}.{GOLD_SCHEMA}.geocoder_enriched"
)

print(f"✅ Created: {GOLD_CATALOG}.{GOLD_SCHEMA}.geocoder_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Show all created tables
print("=" * 70)
print("GOLD LAYER TABLES CREATED")
print("=" * 70)

gold_tables = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_SCHEMA}").collect()

for table in gold_tables:
    table_name = table["tableName"]
    count = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{table_name}").count()
    print(f"🏆 {table_name}: {count:,} rows")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference: Gold Tables Created
# MAGIC 
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `places_intelligence` | Full place data with scores, rankings, and distances |
# MAGIC | `travel_analysis` | Route analysis with travel times, costs, and traffic impact |
# MAGIC | `location_summary` | Aggregate statistics across all places |
# MAGIC | `places_by_type` | Statistics grouped by place type |
# MAGIC | `top_rated_places` | Top 50 places by weighted rating |
# MAGIC | `nearest_places` | Top 50 closest places to reference location |
# MAGIC | `geocoder_enriched` | Parsed and enriched geocoder data |
# MAGIC 
# MAGIC ## Calculated Columns
# MAGIC 
# MAGIC ### Places Intelligence
# MAGIC - `distance_from_reference_km`: Haversine distance to reference point
# MAGIC - `weighted_rating`: Bayesian average rating (adjusted for review count)
# MAGIC - `rating_confidence`: high/medium/low based on review count
# MAGIC - `service_score`: Count of service features (takeout, delivery, etc.)
# MAGIC - `accessibility_score`: Count of accessibility features
# MAGIC - `overall_score`: Combined distance + rating score
# MAGIC - `rating_rank`: Rank by weighted rating
# MAGIC - `distance_rank`: Rank by distance from reference
# MAGIC 
# MAGIC ### Travel Analysis
# MAGIC - `distance_km`: Distance in kilometers
# MAGIC - `duration_minutes`: Travel time in minutes
# MAGIC - `traffic_delay_minutes`: Additional time due to traffic
# MAGIC - `traffic_delay_percent`: Traffic impact as percentage
# MAGIC - `average_speed_kmh`: Average speed for route
# MAGIC - `estimated_fuel_cost_eur`: Estimated fuel cost (8L/100km @ €1.60/L)
# MAGIC - `route_category`: short/medium/long/very_long
# MAGIC - `duration_category`: quick/moderate/long/very_long

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top rated places within 5km
# MAGIC -- SELECT * FROM googlemaps.googlemaps_gold.places_intelligence 
# MAGIC -- WHERE distance_from_reference_km < 5 
# MAGIC -- ORDER BY weighted_rating DESC LIMIT 10;
# MAGIC 
# MAGIC -- Travel routes with significant traffic
# MAGIC -- SELECT * FROM googlemaps.googlemaps_gold.travel_analysis 
# MAGIC -- WHERE traffic_delay_percent > 20 
# MAGIC -- ORDER BY traffic_delay_minutes DESC;
# MAGIC 
# MAGIC -- Summary statistics
# MAGIC -- SELECT * FROM googlemaps.googlemaps_gold.location_summary;
# MAGIC 
# MAGIC -- Places by type breakdown
# MAGIC -- SELECT * FROM googlemaps.googlemaps_gold.places_by_type ORDER BY place_count DESC;

