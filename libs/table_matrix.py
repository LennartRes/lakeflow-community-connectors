"""
Table Matrix Expander for LakeFlow Connect.

Expands a compact parameter matrix into multiple table configurations,
reducing boilerplate for multi-location, multi-repo, or multi-parameter ingestions.

Example:
    Instead of defining 10 separate table objects for 10 cities, define a matrix:

    matrix_spec = {
        "source_table": "places",
        "parameters": [
            {"location_address": "Berlin, Germany", "radius": "5000"},
            {"location_address": "Munich, Germany", "radius": "5000"},
            {"location_address": "Hamburg, Germany", "radius": "5000"},
        ],
        "common_config": {
            "included_types": "restaurant",
            "language_code": "en",
            "scd_type": "SCD_TYPE_1",
        },
        "destination": {
            "catalog": "googlemaps",
            "schema": "places",
            "table_template": "restaurants_{location_slug}",  # {param_name} substitution
        }
    }

This module is connector-agnostic and can be reused by any connector:
- Google Maps: Multiple cities/locations
- GitHub: Multiple repositories
- Stripe: Multiple time periods or accounts
- HubSpot: Multiple lists or segments
"""

import re
from typing import Dict, List, Any, Optional
from copy import deepcopy


def _slugify(text: str) -> str:
    """
    Convert text to a valid table name slug.

    Args:
        text: The text to slugify

    Returns:
        A lowercase string with non-alphanumeric characters replaced by underscores

    Examples:
        >>> _slugify("Berlin, Germany")
        'berlin_germany'
        >>> _slugify("New York City")
        'new_york_city'
        >>> _slugify("München (Bavaria)")
        'm_nchen_bavaria'
    """
    # Lowercase and replace non-alphanumeric with underscore
    slug = re.sub(r'[^a-z0-9]+', '_', text.lower())
    # Remove leading/trailing underscores
    slug = slug.strip('_')
    # Collapse multiple underscores
    slug = re.sub(r'_+', '_', slug)
    return slug


def _substitute_template(template: str, params: Dict[str, str]) -> str:
    """
    Substitute {param_name} and {param_name_slug} placeholders in template.

    For each parameter, two substitution forms are available:
        - {param_name}: raw value
        - {param_name_slug}: slugified value (safe for table names)

    Args:
        template: The template string with placeholders
        params: Dictionary of parameter names to values

    Returns:
        The template with all placeholders substituted

    Example:
        >>> _substitute_template("restaurants_{location_address_slug}", {"location_address": "Berlin, Germany"})
        'restaurants_berlin_germany'
    """
    result = template
    for key, value in params.items():
        # Raw substitution
        result = result.replace(f"{{{key}}}", str(value))
        # Slugified substitution
        result = result.replace(f"{{{key}_slug}}", _slugify(str(value)))
    return result


def expand_matrix(matrix_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Expand a matrix specification into a list of table objects.

    Args:
        matrix_spec: Dictionary containing:
            - source_table (required): The connector table name (e.g., "places")
            - parameters (required): List of parameter dictionaries to expand
            - common_config (optional): Config applied to all generated tables
            - destination (optional): Destination template with:
                - catalog: Target catalog
                - schema: Target schema
                - table_template: Template with {param} placeholders

    Returns:
        List of standard table objects ready for pipeline spec

    Raises:
        ValueError: If source_table is missing or parameters is empty

    Example:
        >>> matrix = {
        ...     "source_table": "places",
        ...     "parameters": [
        ...         {"location_address": "Berlin", "radius": "5000"},
        ...         {"location_address": "Munich", "radius": "5000"},
        ...     ],
        ...     "common_config": {"included_types": "restaurant"},
        ...     "destination": {
        ...         "catalog": "my_catalog",
        ...         "schema": "my_schema",
        ...         "table_template": "restaurants_{location_address_slug}"
        ...     }
        ... }
        >>> tables = expand_matrix(matrix)
        >>> len(tables)
        2
    """
    source_table = matrix_spec.get("source_table")
    if not source_table:
        raise ValueError("matrix_spec must include 'source_table'")

    parameters = matrix_spec.get("parameters", [])
    if not parameters:
        raise ValueError("matrix_spec must include non-empty 'parameters' list")

    common_config = matrix_spec.get("common_config", {})
    destination = matrix_spec.get("destination", {})

    expanded_tables = []

    for idx, params in enumerate(parameters):
        # Build table configuration by merging common + specific params
        table_config = deepcopy(common_config)
        table_config.update(params)

        # Build the table object
        table_obj = {
            "table": {
                "source_table": source_table,
                "table_configuration": table_config,
            }
        }

        # Add destination if template provided
        if destination:
            if destination.get("catalog"):
                table_obj["table"]["destination_catalog"] = destination["catalog"]
            if destination.get("schema"):
                table_obj["table"]["destination_schema"] = destination["schema"]

            # Generate destination table name from template
            table_template = destination.get("table_template")
            if table_template:
                table_name = _substitute_template(table_template, params)
                table_obj["table"]["destination_table"] = table_name
            else:
                # Default: source_table + index
                table_obj["table"]["destination_table"] = f"{source_table}_{idx}"

        expanded_tables.append(table_obj)

    return expanded_tables


def expand_pipeline_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expand a pipeline spec that may contain matrix definitions.

    Supports mixed specs with both regular 'table' objects and 'matrix' objects.
    This is the main entry point for using matrix expansion in your pipeline.

    Args:
        spec: Pipeline spec with optional matrix definitions

    Returns:
        Standard pipeline spec with all matrices expanded to table objects

    Example:
        >>> spec = {
        ...     "connection_name": "googlemaps",
        ...     "objects": [
        ...         {"table": {"source_table": "geocoder", "table_configuration": {...}}},
        ...         {"matrix": {
        ...             "source_table": "places",
        ...             "parameters": [...],
        ...         }}
        ...     ]
        ... }
        >>> expanded = expand_pipeline_spec(spec)
        >>> # Now expanded["objects"] contains all matrix entries expanded to tables
    """
    if "objects" not in spec:
        return spec

    expanded_objects = []

    for obj in spec["objects"]:
        if "table" in obj:
            # Regular table object - pass through unchanged
            expanded_objects.append(obj)
        elif "matrix" in obj:
            # Matrix object - expand to multiple tables
            matrix_tables = expand_matrix(obj["matrix"])
            expanded_objects.extend(matrix_tables)
        else:
            # Unknown object type - pass through (will fail validation later)
            expanded_objects.append(obj)

    # Return new spec with expanded objects
    return {
        **spec,
        "objects": expanded_objects,
    }


# =============================================================================
# Convenience Helper Functions
# =============================================================================


def generate_city_parameters(
    cities: List[str],
    radius: str = "5000",
    extra_config: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    """
    Helper to generate parameters for multiple cities (Google Maps specific).

    Args:
        cities: List of city names/addresses
        radius: Search radius in meters (default: 5000)
        extra_config: Additional config to add to each parameter set

    Returns:
        List of parameter dictionaries ready for matrix expansion

    Example:
        >>> params = generate_city_parameters(
        ...     ["Berlin, Germany", "Munich, Germany"],
        ...     radius="10000",
        ...     extra_config={"included_types": "restaurant"}
        ... )
        >>> params[0]
        {'location_address': 'Berlin, Germany', 'radius': '10000', 'included_types': 'restaurant'}
    """
    params = []
    for city in cities:
        param = {
            "location_address": city,
            "radius": radius,
        }
        if extra_config:
            param.update(extra_config)
        params.append(param)
    return params


def generate_repo_parameters(
    repos: List[Dict[str, str]],
    extra_config: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    """
    Helper to generate parameters for multiple GitHub repositories.

    Args:
        repos: List of repo dicts with 'owner' and 'repo' keys
        extra_config: Additional config to add to each parameter set

    Returns:
        List of parameter dictionaries ready for matrix expansion

    Example:
        >>> params = generate_repo_parameters([
        ...     {"owner": "databricks", "repo": "terraform-provider-databricks"},
        ...     {"owner": "databricks", "repo": "cli"},
        ... ])
    """
    params = []
    for repo in repos:
        param = {
            "owner": repo["owner"],
            "repo": repo["repo"],
        }
        if extra_config:
            param.update(extra_config)
        params.append(param)
    return params


def generate_date_range_parameters(
    start_dates: List[str],
    extra_config: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]:
    """
    Helper to generate parameters for multiple date ranges.

    Args:
        start_dates: List of start date strings (ISO format recommended)
        extra_config: Additional config to add to each parameter set

    Returns:
        List of parameter dictionaries ready for matrix expansion

    Example:
        >>> params = generate_date_range_parameters([
        ...     "2024-01-01",
        ...     "2024-02-01",
        ...     "2024-03-01",
        ... ])
    """
    params = []
    for start_date in start_dates:
        param = {
            "start_date": start_date,
        }
        if extra_config:
            param.update(extra_config)
        params.append(param)
    return params

