# Lakeflow Google Maps Community Connector

This documentation provides setup instructions and reference information for the Google Maps source connector. This connector integrates with multiple Google Maps Platform APIs to retrieve location data, geocoding results, and travel distance/time calculations.

## Prerequisites

- **Google Cloud Project**: An active Google Cloud project with billing enabled
- **APIs Enabled**: The following APIs must be enabled in your Google Cloud project:
  - **Places API (New)** — for the `places` table
  - **Geocoding API** — for the `geocoder` table
  - **Distance Matrix API** — for the `distance_matrix` table
- **API Key**: A Google Maps Platform API key with permissions for the APIs you plan to use

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameter in your connector options:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string | Yes | Google Maps Platform API key | `AIzaSyD...` |

> **Note**: The `externalOptionsAllowList` is automatically configured via the connector specification (`connector_spec.yaml`). You do not need to set it manually.

### Obtaining Your API Key

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable billing for your project (required for Google Maps APIs)
4. Navigate to **APIs & Services** → **Library**
5. Search for and enable the APIs you need:
   - "Places API (New)" for place searches
   - "Geocoding API" for address/coordinate conversions
   - "Distance Matrix API" for travel distance calculations
6. Navigate to **APIs & Services** → **Credentials**
7. Click **Create Credentials** → **API Key**
8. (Recommended) Restrict the API key:
   - Click on the newly created key
   - Under "API restrictions", select "Restrict key"
   - Select only the APIs you plan to use

**Important**: Keep your API key secure and never expose it in client-side code.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Google Maps connector supports **three static objects**:

| Table Name | Description | Primary Key | Ingestion Type |
|------------|-------------|-------------|----------------|
| `places` | Location data from Google's database of over 200 million places | `id` | `snapshot` |
| `geocoder` | Address-to-coordinates (geocoding) and coordinates-to-address (reverse geocoding) | `place_id` | `snapshot` |
| `distance_matrix` | Travel distance and time calculations between origins and destinations | `origin_index`, `destination_index` | `snapshot` |

---

### `places`

The `places` object retrieves location data using **two search modes**:

1. **Text Search**: Search by text query (e.g., "restaurants in Seattle")
2. **Nearby Search**: Search within a geographic area defined by coordinates and radius

Each place record includes:

- Unique place identifier
- Name and address information
- Geographic coordinates (latitude/longitude)
- Business information (hours, ratings, reviews count)
- Contact information (phone, website)
- Service attributes (takeout, delivery, dine-in, etc.)
- Accessibility and parking information

**Ingestion Type**: Snapshot only. The Places API does not support incremental synchronization—each query returns the current state of matching places.

**Result Limit**: The API returns a maximum of 60 results per search query (3 pages of 20 results).

#### Table Options for `places` - Text Search Mode

Use `text_query` to search by text:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `text_query` | string | Yes* | The search query for places | `"restaurants in Seattle"` |
| `language_code` | string | No | Language code for results | `"en"` |
| `max_result_count` | string | No | Maximum results per page (1-20, default 20) | `"20"` |
| `included_type` | string | No | Restrict to a specific place type | `"restaurant"` |
| `min_rating` | string | No | Minimum average rating filter (1.0-5.0) | `"4.0"` |
| `open_now` | string | No | Only return places currently open | `"true"` |
| `region_code` | string | No | Region code for biasing results | `"US"` |

#### Table Options for `places` - Nearby Search Mode

Search within a geographic circle using either an address OR coordinates:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `location_address` | string | Yes* | Address/city to search near (auto-geocoded) | `"Alexanderplatz, Berlin"` |
| `latitude` | string | Yes* | Center latitude for nearby search | `"52.5200"` |
| `longitude` | string | Yes* | Center longitude for nearby search | `"13.4050"` |
| `radius` | string | Yes | Search radius in meters (max 50000) | `"1000"` |
| `included_types` | string | No | Comma-separated place types to include | `"restaurant,cafe"` |
| `excluded_types` | string | No | Comma-separated place types to exclude | `"bar"` |
| `language_code` | string | No | Language code for results | `"en"` |
| `max_result_count` | string | No | Maximum results (1-20, default 20) | `"20"` |
| `rank_preference` | string | No | Ranking: `DISTANCE` or `POPULARITY` | `"DISTANCE"` |

*Requires `radius` plus ONE of: `location_address` OR (`latitude` + `longitude`). The `location_address` option internally geocodes the address to coordinates using the Geocoding API.

#### Schema Highlights for `places`

**Core Fields:**
- `id`: Unique Google Place ID (primary key)
- `formattedAddress`: Human-readable address
- `rating`: Average user rating (1.0 to 5.0)
- `userRatingCount`: Total number of user ratings
- `businessStatus`: `OPERATIONAL`, `CLOSED_TEMPORARILY`, or `CLOSED_PERMANENTLY`
- `priceLevel`: Price indicator from `PRICE_LEVEL_FREE` to `PRICE_LEVEL_VERY_EXPENSIVE`

**Nested Structures:**
- `displayName`: Contains `text` (place name) and `languageCode`
- `location`: Contains `latitude` and `longitude`
- `addressComponents`: Array of address components with types
- `currentOpeningHours` / `regularOpeningHours`: Opening hours with periods and descriptions
- `accessibilityOptions`: Wheelchair accessibility features
- `parkingOptions`: Parking availability information
- `paymentOptions`: Accepted payment methods

**Service Attributes (boolean):**
- `takeout`, `delivery`, `dineIn`, `reservable`
- `servesBreakfast`, `servesLunch`, `servesDinner`, `servesBrunch`
- `servesBeer`, `servesWine`, `servesVegetarianFood`
- `outdoorSeating`, `liveMusic`, `goodForGroups`, `allowsDogs`

---

### `geocoder`

The `geocoder` object provides geocoding results that convert addresses into geographic coordinates (forward geocoding) or convert coordinates into human-readable addresses (reverse geocoding). Each result includes:

- Formatted address string
- Address components (street, city, state, country, postal code)
- Geographic coordinates (latitude/longitude)
- Location type accuracy indicator
- Viewport bounds for displaying the location
- Place ID for cross-referencing with Places API
- Plus Code (Open Location Code)

**Ingestion Type**: Snapshot only. The Geocoding API is a stateless query service—each request returns current geocoding data.

**Result Limit**: Each request returns all matching results (typically 1-5 results ordered by relevance).

#### Table Options for `geocoder`

One of `address`, `latlng`, or `place_id` is **required**:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `address` | string | Yes* | Address to geocode (forward geocoding) | `"1600 Amphitheatre Parkway, Mountain View, CA"` |
| `latlng` | string | Yes* | Coordinates to reverse geocode (format: "lat,lng") | `"37.4224764,-122.0842499"` |
| `place_id` | string | Yes* | Place ID to geocode | `"ChIJ2eUgeAK6j4ARbn5u_wAGqWA"` |
| `language` | string | No | Language code for results | `"en"` |
| `region` | string | No | Region code to bias results | `"us"` |
| `bounds` | string | No | Bounding box to bias results (format: "lat,lng\|lat,lng") | `"34.0,-118.5\|34.3,-118.1"` |
| `components` | string | No | Component filter (format: "component:value\|...") | `"country:US"` |
| `result_type` | string | No | Filter by address type (reverse geocoding only) | `"street_address"` |
| `location_type` | string | No | Filter by location type (reverse geocoding only) | `"ROOFTOP"` |

*One of `address`, `latlng`, or `place_id` must be provided.

#### Schema Highlights for `geocoder`

**Core Fields:**
- `place_id`: Unique identifier (primary key), can be used with Places API
- `formatted_address`: Human-readable address string
- `types`: Array of address type indicators (e.g., "street_address", "locality")
- `partial_match`: Boolean indicating if geocoder did not find an exact match

**Nested Structures:**
- `geometry.location`: Contains `lat` and `lng` coordinates
- `geometry.location_type`: Accuracy indicator (`ROOFTOP`, `RANGE_INTERPOLATED`, `GEOMETRIC_CENTER`, `APPROXIMATE`)
- `geometry.viewport`: Recommended viewport bounds with `northeast` and `southwest` corners
- `geometry.bounds`: Optional bounding box for area results
- `address_components`: Array of components with `long_name`, `short_name`, and `types`
- `plus_code`: Contains `global_code` and `compound_code`

**Location Type Values:**
| Value | Description |
|-------|-------------|
| `ROOFTOP` | Precise geocode with street address precision |
| `RANGE_INTERPOLATED` | Approximate location between two precise points |
| `GEOMETRIC_CENTER` | Geometric center of a result (e.g., polyline) |
| `APPROXIMATE` | Approximate location (e.g., city center) |

---

### `distance_matrix`

The `distance_matrix` object provides travel distance and time for a matrix of origins and destinations. Each element in the response represents one origin-destination pair and includes:

- Travel distance (in meters and human-readable text)
- Travel duration (in seconds and human-readable text)
- Duration in traffic (for driving mode with departure time)
- Transit fare information (for transit mode)
- Status indicating if the route was found

**Ingestion Type**: Snapshot only. The Distance Matrix API calculates distances/durations based on current road network and real-time traffic conditions.

**Result Limit**: 
- Maximum 25 origins per request
- Maximum 25 destinations per request
- Maximum 100 elements (origins × destinations) per request

#### Table Options for `distance_matrix`

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `origins` | string | Yes | Pipe-separated list of origins | `"Seattle, WA\|Portland, OR"` |
| `destinations` | string | Yes | Pipe-separated list of destinations | `"San Francisco, CA\|Los Angeles, CA"` |
| `mode` | string | No | Travel mode: `driving`, `walking`, `bicycling`, `transit` | `"driving"` |
| `language` | string | No | Language code for results | `"en"` |
| `region` | string | No | Region code to bias results | `"us"` |
| `avoid` | string | No | Features to avoid: `tolls\|highways\|ferries\|indoor` | `"tolls\|highways"` |
| `units` | string | No | Unit system: `metric` or `imperial` | `"metric"` |
| `departure_time` | string | No | Departure time as Unix timestamp or `"now"` | `"now"` |
| `arrival_time` | string | No | Arrival time as Unix timestamp (transit only) | `"1609459200"` |
| `traffic_model` | string | No | Traffic model: `best_guess`, `pessimistic`, `optimistic` | `"best_guess"` |
| `transit_mode` | string | No | Transit modes: `bus\|subway\|train\|tram\|rail` | `"subway\|train"` |
| `transit_routing_preference` | string | No | Transit preference: `less_walking`, `fewer_transfers` | `"less_walking"` |

**Origin/Destination Formats:**
- Address: `"1600 Amphitheatre Parkway, Mountain View, CA"`
- Coordinates: `"37.4224764,-122.0842499"`
- Place ID: `"place_id:ChIJ2eUgeAK6j4ARbn5u_wAGqWA"`

#### Schema Highlights for `distance_matrix`

The response is flattened so each record represents one origin-destination pair:

**Core Fields:**
- `origin_index`: Index of the origin in the request (part of primary key)
- `destination_index`: Index of the destination in the request (part of primary key)
- `origin_address`: Address as interpreted by the API
- `destination_address`: Address as interpreted by the API
- `status`: Element status (`OK`, `NOT_FOUND`, `ZERO_RESULTS`)

**Nested Structures:**
- `distance`: Contains `value` (meters) and `text` (human-readable)
- `duration`: Contains `value` (seconds) and `text` (human-readable)
- `duration_in_traffic`: Same structure, only present with `departure_time` and driving mode
- `fare`: Contains `currency`, `value`, and `text` (transit mode only)

**Element Status Codes:**
| Status | Description |
|--------|-------------|
| `OK` | Valid result for this origin-destination pair |
| `NOT_FOUND` | Origin and/or destination could not be geocoded |
| `ZERO_RESULTS` | No route could be found between origin and destination |

---

## Data Type Mapping

| Google Maps API Type | Spark Type | Notes |
|---------------------|------------|-------|
| string | StringType | Direct mapping |
| number (lat/lng, rating) | DoubleType | Floating-point values |
| integer (counts, meters, seconds) | LongType | Numeric values |
| boolean | BooleanType | Flags like `takeout`, `delivery`, `partial_match` |
| object | StructType | Nested structures like `displayName`, `location`, `geometry` |
| array | ArrayType | Lists such as `types`, `addressComponents` |
| enum (string) | StringType | `businessStatus`, `priceLevel`, `location_type` |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`)
2. Configure the appropriate table options for each object you want to ingest

**Example `pipeline_spec` with multiple tables:**

```json
{
  "pipeline_spec": {
    "connection_name": "googlemaps_connection",
    "object": [
      {
        "table": {
          "source_table": "places",
          "text_query": "coffee shops in Seattle",
          "language_code": "en",
          "min_rating": "4.0"
        }
      },
      {
        "table": {
          "source_table": "places",
          "text_query": "restaurants in San Francisco",
          "included_type": "restaurant"
        }
      },
      {
        "table": {
          "source_table": "places",
          "latitude": "52.5200",
          "longitude": "13.4050",
          "radius": "1500",
          "included_types": "gas_station",
          "rank_preference": "DISTANCE",
          "language_code": "de"
        }
      },
      {
        "table": {
          "source_table": "places",
          "location_address": "Eiffel Tower, Paris, France",
          "radius": "500",
          "included_types": "restaurant,cafe",
          "excluded_types": "bar",
          "max_result_count": "20"
        }
      },
      {
        "table": {
          "source_table": "geocoder",
          "address": "1600 Amphitheatre Parkway, Mountain View, CA",
          "language": "en"
        }
      },
      {
        "table": {
          "source_table": "geocoder",
          "latlng": "47.6062,-122.3321",
          "language": "en"
        }
      },
      {
        "table": {
          "source_table": "distance_matrix",
          "origins": "Seattle, WA|Portland, OR",
          "destinations": "San Francisco, CA|Los Angeles, CA",
          "mode": "driving",
          "units": "metric"
        }
      },
      {
        "table": {
          "source_table": "distance_matrix",
          "origins": "New York, NY",
          "destinations": "Boston, MA|Philadelphia, PA|Washington, DC",
          "mode": "transit",
          "departure_time": "now"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Google Maps `api_key`
- Each `table` entry requires `source_table` and the table-specific required options
- You can have multiple entries for the same table with different parameters

3. (Optional) Customize the source connector code if needed for special use cases

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin with a focused query to validate configuration
- **Use Specific Queries**: More specific queries yield more relevant results and reduce costs
- **Consider Costs**: All Google Maps APIs are paid services—balance data freshness with API usage
- **Apply Filters**: Use filters to narrow results and reduce unnecessary API calls
- **Batch Distance Matrix Requests**: Group multiple origins/destinations in a single request rather than making many single-pair requests

#### Scheduling Considerations

Since all tables use snapshot ingestion:
- Each run performs a complete refresh of the data
- Schedule syncs based on how frequently the data changes for your use case
- Consider staggering syncs across different queries to spread API costs
- For `distance_matrix` with traffic data, schedule during relevant time windows

## Multi-Instance Ingestion Support

The Google Maps connector supports **multiple ingestions from the same source table** within
a single pipeline. For example, you can query the `places` table multiple times with different
parameters (different cities, different place types) and each will create a separate destination table.

This is enabled by the index-based pipeline architecture that uses the **destination table name**
(not source table name) for unique view naming, avoiding conflicts when the same API endpoint
is queried multiple times.

---

## Matrix-Based Table Generation (Advanced)

For scenarios where you need to ingest the same data type across multiple locations,
the **table matrix** feature allows you to generate multiple destination tables from
a single connector table using parameter matrices.

### The Problem

Without matrix expansion, ingesting restaurants for 10 cities requires 10 separate table
configurations—each nearly identical except for the location:

```python
# ❌ Repetitive: 10 table configurations that are 90% identical
pipeline_spec = {
    "objects": [
        {"table": {"source_table": "places", "table_configuration": {"location_address": "Berlin, Germany", "radius": "5000", "included_types": "restaurant", ...}}},
        {"table": {"source_table": "places", "table_configuration": {"location_address": "Munich, Germany", "radius": "5000", "included_types": "restaurant", ...}}},
        {"table": {"source_table": "places", "table_configuration": {"location_address": "Hamburg, Germany", "radius": "5000", "included_types": "restaurant", ...}}},
        # ... 7 more nearly identical entries
    ]
}
```

### The Solution

Use the `libs/table_matrix` module to define a single matrix that expands automatically:

```python
from libs.table_matrix import expand_pipeline_spec, generate_city_parameters

# ✅ Concise: 1 matrix definition generates 10 tables
pipeline_spec = expand_pipeline_spec({
    "connection_name": "googlemaps",
    "objects": [
        {
            "matrix": {
                "source_table": "places",
                "parameters": generate_city_parameters([
                    "Berlin, Germany",
                    "Munich, Germany",
                    "Hamburg, Germany",
                    "Frankfurt, Germany",
                    "Cologne, Germany",
                    # ... add as many cities as needed
                ], radius="5000"),
                "common_config": {
                    "included_types": "restaurant",
                    "language_code": "en",
                    "scd_type": "SCD_TYPE_1",
                },
                "destination": {
                    "catalog": "googlemaps",
                    "schema": "places",
                    "table_template": "restaurants_{location_address_slug}",
                }
            }
        }
    ]
})
```

### Matrix Specification Reference

| Field | Required | Description |
|-------|----------|-------------|
| `source_table` | Yes | The connector table name (e.g., `"places"`, `"geocoder"`) |
| `parameters` | Yes | List of parameter dictionaries—each generates one table |
| `common_config` | No | Configuration applied to ALL generated tables |
| `destination` | No | Destination template for catalog, schema, and table name |

### Destination Template Variables

In `table_template`, use these placeholders:

| Placeholder | Description | Example Input | Output |
|-------------|-------------|---------------|--------|
| `{param_name}` | Raw parameter value | `location_address="Berlin"` | `Berlin` |
| `{param_name_slug}` | Slugified value (lowercase, underscores) | `location_address="Berlin, Germany"` | `berlin_germany` |

**Example:**
```python
"table_template": "restaurants_{location_address_slug}"
# With location_address="Munich, Germany" → "restaurants_munich_germany"
```

### Mixing Regular Tables and Matrices

You can combine regular `table` objects with `matrix` objects in the same spec:

```python
pipeline_spec = expand_pipeline_spec({
    "connection_name": "googlemaps",
    "objects": [
        # Regular table: Single geocoder query
        {
            "table": {
                "source_table": "geocoder",
                "destination_table": "office_location",
                "table_configuration": {"address": "123 Main St"}
            }
        },
        # Matrix: Multiple restaurant tables
        {
            "matrix": {
                "source_table": "places",
                "parameters": [...],
                "common_config": {...},
                "destination": {...}
            }
        },
        # Another matrix: Gas stations
        {
            "matrix": {
                "source_table": "places",
                "parameters": [...],
                "common_config": {"included_types": "gas_station"},
                "destination": {...}
            }
        },
    ]
})
```

### Helper Functions

The module provides convenience helpers for common patterns:

```python
from libs.table_matrix import (
    generate_city_parameters,      # Google Maps: multiple cities
    generate_repo_parameters,      # GitHub: multiple repositories
    generate_date_range_parameters # Any: multiple time periods
)

# Example: Generate parameters for German cities
params = generate_city_parameters(
    cities=["Berlin", "Munich", "Hamburg"],
    radius="5000",
    extra_config={"language_code": "de"}
)
```

### Reusability Across Connectors

The table matrix feature is **connector-agnostic**. Any Lakeflow connector can use it:

| Connector | Use Case | Parameter |
|-----------|----------|-----------|
| **Google Maps** | Multiple cities | `location_address` |
| **GitHub** | Multiple repositories | `owner`, `repo` |
| **Stripe** | Multiple time periods | `start_date` |
| **HubSpot** | Multiple lists/segments | `list_id` |

### Example Pipeline

See the full example at: `sources/googlemaps/examples/example_matrix_ingest.py`

---

## Troubleshooting

**Common Issues:**

**Authentication Errors (401/403):**
- Verify that the `api_key` is correct and not expired
- Ensure the required API is enabled in your Google Cloud project:
  - Places API (New) for `places`
  - Geocoding API for `geocoder`
  - Distance Matrix API for `distance_matrix`
- Check that billing is enabled for your project

**No Results Returned:**
- For `places`: Verify the `text_query` is specific enough and matches existing places
- For `geocoder`: Check that the address format is correct or coordinates are valid
- For `distance_matrix`: Ensure origins and destinations are valid locations

**Missing Required Parameter Error:**
- `places` requires ONE of:
  - `text_query` for text search mode, OR
  - `location_address` + `radius` for nearby search (address-based), OR
  - `latitude` + `longitude` + `radius` for nearby search (coordinates)
- `geocoder` requires one of `address`, `latlng`, or `place_id`
- `distance_matrix` requires both `origins` and `destinations`

**Conflicting Parameters Error (places):**
- Cannot use `text_query` together with nearby search parameters (`location_address`, `latitude`, `longitude`, `radius`)
- Cannot use both `location_address` and `latitude`/`longitude` — choose one location method

**Rate Limiting / Quota Exceeded:**
- All APIs have usage quotas—check your Google Cloud Console for current limits
- Reduce sync frequency or spread queries across multiple pipeline runs
- For `distance_matrix`, minimize the number of elements (origins × destinations)

**REQUEST_DENIED Errors:**
- The specific API is not enabled in your Google Cloud project
- Enable the required API in the Google Cloud Console API Library

**ZERO_RESULTS (Distance Matrix):**
- No route could be found between the specified origin and destination
- This can happen for locations across bodies of water or in different continents without connecting roads

**Unexpected Schema/Data:**
- Nested objects require appropriate handling downstream
- Some fields may be `null` if information is not available
- `duration_in_traffic` only appears when `departure_time` is specified with driving mode
- `fare` only appears for transit mode in supported regions

## References

- **Connector Implementation**: `sources/googlemaps/googlemaps.py`
- **Connector Specification**: `sources/googlemaps/connector_spec.yaml`
- **API Documentation**: `sources/googlemaps/googlemaps_api_doc.md`
- **Official Google Documentation**:
  - [Places API Overview](https://developers.google.com/maps/documentation/places/web-service)
  - [Text Search (New)](https://developers.google.com/maps/documentation/places/web-service/text-search)
  - [Nearby Search (New)](https://developers.google.com/maps/documentation/places/web-service/nearby-search)
  - [Place Data Fields](https://developers.google.com/maps/documentation/places/web-service/data-fields)
  - [Place Types](https://developers.google.com/maps/documentation/places/web-service/place-types)
  - [Geocoding API](https://developers.google.com/maps/documentation/geocoding)
  - [Distance Matrix API](https://developers.google.com/maps/documentation/distance-matrix)
  - [Usage and Billing](https://developers.google.com/maps/documentation/places/web-service/usage-and-billing)
