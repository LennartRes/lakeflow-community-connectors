"""Tests for table_matrix module."""

import pytest
from libs.table_matrix import (
    expand_matrix,
    expand_pipeline_spec,
    generate_city_parameters,
    generate_repo_parameters,
    generate_date_range_parameters,
    _slugify,
    _substitute_template,
)


class TestSlugify:
    """Tests for the _slugify helper function."""

    def test_simple_city(self):
        assert _slugify("Berlin") == "berlin"

    def test_city_with_country(self):
        assert _slugify("Berlin, Germany") == "berlin_germany"

    def test_city_with_spaces(self):
        assert _slugify("New York City") == "new_york_city"

    def test_special_characters(self):
        assert _slugify("München (Bavaria)") == "m_nchen_bavaria"

    def test_numbers_preserved(self):
        assert _slugify("District 9") == "district_9"

    def test_leading_trailing_special_chars(self):
        assert _slugify("  Berlin!  ") == "berlin"

    def test_multiple_spaces(self):
        assert _slugify("San   Francisco") == "san_francisco"


class TestSubstituteTemplate:
    """Tests for the _substitute_template helper function."""

    def test_raw_substitution(self):
        result = _substitute_template(
            "places_{location_address}",
            {"location_address": "Berlin"}
        )
        assert result == "places_Berlin"

    def test_slug_substitution(self):
        result = _substitute_template(
            "places_{location_address_slug}",
            {"location_address": "Berlin, Germany"}
        )
        assert result == "places_berlin_germany"

    def test_multiple_params(self):
        result = _substitute_template(
            "{city_slug}_{type}",
            {"city": "Munich", "type": "restaurant"}
        )
        assert result == "munich_restaurant"

    def test_mixed_raw_and_slug(self):
        result = _substitute_template(
            "{city_slug}_radius_{radius}",
            {"city": "New York", "radius": "5000"}
        )
        assert result == "new_york_radius_5000"

    def test_no_placeholders(self):
        result = _substitute_template("static_name", {"key": "value"})
        assert result == "static_name"

    def test_missing_placeholder_unchanged(self):
        result = _substitute_template(
            "{missing_param}_test",
            {"other_param": "value"}
        )
        assert result == "{missing_param}_test"


class TestExpandMatrix:
    """Tests for the expand_matrix function."""

    def test_basic_expansion(self):
        matrix = {
            "source_table": "places",
            "parameters": [
                {"location_address": "Berlin"},
                {"location_address": "Munich"},
            ],
        }
        result = expand_matrix(matrix)

        assert len(result) == 2
        assert result[0]["table"]["source_table"] == "places"
        assert result[0]["table"]["table_configuration"]["location_address"] == "Berlin"
        assert result[1]["table"]["table_configuration"]["location_address"] == "Munich"

    def test_common_config_merged(self):
        matrix = {
            "source_table": "places",
            "parameters": [
                {"location_address": "Berlin"},
            ],
            "common_config": {
                "radius": "5000",
                "included_types": "restaurant",
            },
        }
        result = expand_matrix(matrix)

        config = result[0]["table"]["table_configuration"]
        assert config["location_address"] == "Berlin"
        assert config["radius"] == "5000"
        assert config["included_types"] == "restaurant"

    def test_param_overrides_common_config(self):
        """Parameter values should override common_config values."""
        matrix = {
            "source_table": "places",
            "parameters": [
                {"location_address": "Berlin", "radius": "10000"},
            ],
            "common_config": {
                "radius": "5000",
            },
        }
        result = expand_matrix(matrix)

        config = result[0]["table"]["table_configuration"]
        assert config["radius"] == "10000"  # Overridden by parameter

    def test_destination_template(self):
        matrix = {
            "source_table": "places",
            "parameters": [
                {"location_address": "Berlin, Germany"},
            ],
            "destination": {
                "catalog": "my_catalog",
                "schema": "my_schema",
                "table_template": "restaurants_{location_address_slug}",
            },
        }
        result = expand_matrix(matrix)

        assert result[0]["table"]["destination_catalog"] == "my_catalog"
        assert result[0]["table"]["destination_schema"] == "my_schema"
        assert result[0]["table"]["destination_table"] == "restaurants_berlin_germany"

    def test_destination_default_naming(self):
        """Without table_template, tables should be named source_table_index."""
        matrix = {
            "source_table": "places",
            "parameters": [
                {"city": "A"},
                {"city": "B"},
            ],
            "destination": {
                "catalog": "cat",
                "schema": "sch",
            },
        }
        result = expand_matrix(matrix)

        assert result[0]["table"]["destination_table"] == "places_0"
        assert result[1]["table"]["destination_table"] == "places_1"

    def test_partial_destination(self):
        """Only catalog specified, no schema or template."""
        matrix = {
            "source_table": "places",
            "parameters": [{"city": "A"}],
            "destination": {
                "catalog": "only_catalog",
            },
        }
        result = expand_matrix(matrix)

        assert result[0]["table"]["destination_catalog"] == "only_catalog"
        assert "destination_schema" not in result[0]["table"]

    def test_missing_source_table_raises(self):
        with pytest.raises(ValueError, match="source_table"):
            expand_matrix({"parameters": [{}]})

    def test_empty_parameters_raises(self):
        with pytest.raises(ValueError, match="parameters"):
            expand_matrix({"source_table": "places", "parameters": []})

    def test_missing_parameters_raises(self):
        with pytest.raises(ValueError, match="parameters"):
            expand_matrix({"source_table": "places"})


class TestExpandPipelineSpec:
    """Tests for the expand_pipeline_spec function."""

    def test_table_only_passthrough(self):
        """Specs with only table objects should pass through unchanged."""
        spec = {
            "connection_name": "test",
            "objects": [
                {"table": {"source_table": "geocoder"}},
                {"table": {"source_table": "places"}},
            ],
        }
        result = expand_pipeline_spec(spec)

        assert len(result["objects"]) == 2
        assert result["connection_name"] == "test"

    def test_matrix_expansion(self):
        spec = {
            "connection_name": "test",
            "objects": [
                {"matrix": {
                    "source_table": "places",
                    "parameters": [
                        {"city": "A"},
                        {"city": "B"},
                    ],
                }},
            ],
        }
        result = expand_pipeline_spec(spec)

        assert len(result["objects"]) == 2
        assert result["objects"][0]["table"]["source_table"] == "places"
        assert result["objects"][1]["table"]["source_table"] == "places"

    def test_mixed_table_and_matrix(self):
        """Mix of regular table objects and matrix objects."""
        spec = {
            "connection_name": "test",
            "objects": [
                {"table": {"source_table": "geocoder"}},
                {"matrix": {
                    "source_table": "places",
                    "parameters": [
                        {"city": "A"},
                        {"city": "B"},
                    ],
                }},
                {"table": {"source_table": "distance_matrix"}},
            ],
        }
        result = expand_pipeline_spec(spec)

        assert len(result["objects"]) == 4
        assert result["objects"][0]["table"]["source_table"] == "geocoder"
        assert result["objects"][1]["table"]["source_table"] == "places"
        assert result["objects"][2]["table"]["source_table"] == "places"
        assert result["objects"][3]["table"]["source_table"] == "distance_matrix"

    def test_multiple_matrices(self):
        """Multiple matrix objects in the same spec."""
        spec = {
            "connection_name": "test",
            "objects": [
                {"matrix": {
                    "source_table": "places",
                    "parameters": [{"city": "A"}, {"city": "B"}],
                }},
                {"matrix": {
                    "source_table": "geocoder",
                    "parameters": [{"address": "X"}, {"address": "Y"}],
                }},
            ],
        }
        result = expand_pipeline_spec(spec)

        assert len(result["objects"]) == 4

    def test_spec_without_objects(self):
        """Specs without objects key should pass through."""
        spec = {"connection_name": "test"}
        result = expand_pipeline_spec(spec)
        assert result == spec

    def test_preserves_other_spec_fields(self):
        """Other fields in the spec should be preserved."""
        spec = {
            "connection_name": "test",
            "some_other_field": "value",
            "objects": [{"table": {"source_table": "x"}}],
        }
        result = expand_pipeline_spec(spec)
        assert result["some_other_field"] == "value"


class TestGenerateCityParameters:
    """Tests for the generate_city_parameters helper."""

    def test_basic_generation(self):
        cities = ["Berlin", "Munich"]
        result = generate_city_parameters(cities)

        assert len(result) == 2
        assert result[0]["location_address"] == "Berlin"
        assert result[0]["radius"] == "5000"  # default
        assert result[1]["location_address"] == "Munich"

    def test_custom_radius(self):
        result = generate_city_parameters(["Berlin"], radius="10000")
        assert result[0]["radius"] == "10000"

    def test_extra_config(self):
        result = generate_city_parameters(
            ["Berlin"],
            extra_config={"included_types": "restaurant", "language_code": "en"}
        )
        assert result[0]["included_types"] == "restaurant"
        assert result[0]["language_code"] == "en"

    def test_empty_cities(self):
        result = generate_city_parameters([])
        assert result == []


class TestGenerateRepoParameters:
    """Tests for the generate_repo_parameters helper."""

    def test_basic_generation(self):
        repos = [
            {"owner": "databricks", "repo": "cli"},
            {"owner": "apache", "repo": "spark"},
        ]
        result = generate_repo_parameters(repos)

        assert len(result) == 2
        assert result[0]["owner"] == "databricks"
        assert result[0]["repo"] == "cli"

    def test_extra_config(self):
        repos = [{"owner": "test", "repo": "repo"}]
        result = generate_repo_parameters(repos, extra_config={"state": "all"})

        assert result[0]["state"] == "all"


class TestGenerateDateRangeParameters:
    """Tests for the generate_date_range_parameters helper."""

    def test_basic_generation(self):
        dates = ["2024-01-01", "2024-02-01"]
        result = generate_date_range_parameters(dates)

        assert len(result) == 2
        assert result[0]["start_date"] == "2024-01-01"
        assert result[1]["start_date"] == "2024-02-01"

    def test_extra_config(self):
        dates = ["2024-01-01"]
        result = generate_date_range_parameters(
            dates,
            extra_config={"per_page": "100"}
        )
        assert result[0]["per_page"] == "100"


class TestIntegration:
    """Integration tests combining multiple features."""

    def test_full_googlemaps_example(self):
        """Test a realistic Google Maps use case."""
        spec = {
            "connection_name": "googlemaps",
            "objects": [
                # Regular geocoder table
                {
                    "table": {
                        "source_table": "geocoder",
                        "destination_catalog": "catalog",
                        "destination_schema": "schema",
                        "destination_table": "office",
                        "table_configuration": {
                            "address": "123 Main St",
                        }
                    }
                },
                # Matrix for restaurants in multiple cities
                {
                    "matrix": {
                        "source_table": "places",
                        "parameters": generate_city_parameters(
                            ["Berlin, Germany", "Munich, Germany", "Hamburg, Germany"],
                            radius="3000",
                        ),
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
                },
            ],
        }

        result = expand_pipeline_spec(spec)

        # Should have 1 geocoder + 3 restaurant tables
        assert len(result["objects"]) == 4

        # First is unchanged geocoder
        assert result["objects"][0]["table"]["source_table"] == "geocoder"
        assert result["objects"][0]["table"]["destination_table"] == "office"

        # Next 3 are expanded restaurants
        assert result["objects"][1]["table"]["destination_table"] == "restaurants_berlin_germany"
        assert result["objects"][2]["table"]["destination_table"] == "restaurants_munich_germany"
        assert result["objects"][3]["table"]["destination_table"] == "restaurants_hamburg_germany"

        # Check config is properly merged
        berlin_config = result["objects"][1]["table"]["table_configuration"]
        assert berlin_config["location_address"] == "Berlin, Germany"
        assert berlin_config["radius"] == "3000"
        assert berlin_config["included_types"] == "restaurant"
        assert berlin_config["scd_type"] == "SCD_TYPE_1"

    def test_full_github_example(self):
        """Test a realistic GitHub use case."""
        spec = {
            "connection_name": "github",
            "objects": [
                {
                    "matrix": {
                        "source_table": "issues",
                        "parameters": generate_repo_parameters([
                            {"owner": "databricks", "repo": "cli"},
                            {"owner": "databricks", "repo": "terraform-provider-databricks"},
                        ]),
                        "common_config": {
                            "state": "all",
                            "per_page": "100",
                        },
                        "destination": {
                            "catalog": "github",
                            "schema": "issues",
                            "table_template": "{owner_slug}_{repo_slug}_issues",
                        }
                    }
                },
            ],
        }

        result = expand_pipeline_spec(spec)

        assert len(result["objects"]) == 2
        assert result["objects"][0]["table"]["destination_table"] == "databricks_cli_issues"
        assert result["objects"][1]["table"]["destination_table"] == "databricks_terraform_provider_databricks_issues"

