"""Tests for catalog search scoring algorithm and property filtering."""

import pytest

from feast.api.catalog.search import (
    _compute_match_score,
    _fuzzy_overlap,
    _matches_property_filters,
)

MANAGED_TAG = "_catalog_managed"


# ---------------------------------------------------------------------------
# _compute_match_score — exact name match (100)
# ---------------------------------------------------------------------------


class TestExactNameMatch:
    def test_exact_match(self):
        assert _compute_match_score("floods", "floods", "", {}) == 100

    def test_exact_match_case_insensitive(self):
        assert _compute_match_score("Floods", "floods", "", {}) == 100
        assert _compute_match_score("FLOODS", "floods", "", {}) == 100
        assert _compute_match_score("floods", "FLOODS", "", {}) == 100


# ---------------------------------------------------------------------------
# _compute_match_score — substring of name (90)
# ---------------------------------------------------------------------------


class TestSubstringName:
    def test_query_is_substring_of_name(self):
        assert _compute_match_score("flood", "flood_events", "", {}) == 90

    def test_substring_case_insensitive(self):
        assert _compute_match_score("FLOOD", "flood_events", "", {}) == 90

    def test_single_char_substring(self):
        assert _compute_match_score("f", "flood_events", "", {}) == 90


# ---------------------------------------------------------------------------
# _compute_match_score — substring of description (80)
# ---------------------------------------------------------------------------


class TestSubstringDescription:
    def test_query_in_description(self):
        assert (
            _compute_match_score("sensor", "events", "sensor readings daily", {}) == 80
        )

    def test_description_case_insensitive(self):
        assert (
            _compute_match_score("SENSOR", "events", "Sensor Readings Daily", {}) == 80
        )

    def test_empty_description_no_match(self):
        assert _compute_match_score("sensor", "events", "", {}) != 80


# ---------------------------------------------------------------------------
# _compute_match_score — tag key/value match (60)
# ---------------------------------------------------------------------------


class TestTagMatch:
    def test_query_matches_tag_value(self):
        tags = {"domain": "hydrology", MANAGED_TAG: "true"}
        assert _compute_match_score("hydrology", "events", "", tags) == 60

    def test_query_matches_tag_key(self):
        tags = {"domain": "hydrology", MANAGED_TAG: "true"}
        assert _compute_match_score("domain", "events", "", tags) == 60

    def test_managed_tag_is_skipped(self):
        tags = {MANAGED_TAG: "true"}
        assert _compute_match_score("true", "events", "", tags) != 60

    def test_tag_value_case_insensitive(self):
        tags = {"domain": "Hydrology"}
        assert _compute_match_score("hydrology", "events", "", tags) == 60


# ---------------------------------------------------------------------------
# _compute_match_score — fuzzy match (40)
# ---------------------------------------------------------------------------


class TestFuzzyMatch:
    def test_fuzzy_match_above_threshold(self):
        # "flod" vs "flood" — chars {f,l,o,d} vs {f,l,o,d} = 100% overlap
        assert _compute_match_score("flod", "flood", "", {}) == 40

    def test_short_query_no_fuzzy(self):
        # len < 3 should skip fuzzy matching entirely
        assert _compute_match_score("fl", "flood", "", {}) == 0

    def test_fuzzy_below_threshold(self):
        # "xyz" vs "flood" — {x,y,z} & {f,l,o,d} = {} → 0%
        assert _compute_match_score("xyz", "flood", "", {}) == 0


# ---------------------------------------------------------------------------
# _compute_match_score — no match (0)
# ---------------------------------------------------------------------------


class TestNoMatch:
    def test_completely_unrelated(self):
        tags = {"domain": "hydrology"}
        assert _compute_match_score("quantum", "flood_events", "sensor data", tags) == 0

    def test_empty_query_is_substring_of_everything(self):
        # empty string is a substring of any string, so "" in name.lower() == True → 90
        assert _compute_match_score("", "flood_events", "", {}) == 100


# ---------------------------------------------------------------------------
# _compute_match_score — highest tier wins
# ---------------------------------------------------------------------------


class TestHighestTierWins:
    def test_exact_beats_substring(self):
        assert _compute_match_score("floods", "floods", "floods data", {}) == 100

    def test_name_substring_beats_description(self):
        assert (
            _compute_match_score("flood", "flood_events", "flood sensor data", {})
            == 90
        )

    def test_description_beats_tag(self):
        tags = {"domain": "flood"}
        assert (
            _compute_match_score("flood", "events", "flood sensor data", tags) == 80
        )

    def test_tag_beats_fuzzy(self):
        # "flod" would fuzzy-match "events" poorly, but matches tag value
        tags = {"typo": "flod"}
        assert _compute_match_score("flod", "events", "", tags) == 60


# ---------------------------------------------------------------------------
# _fuzzy_overlap
# ---------------------------------------------------------------------------


class TestFuzzyOverlap:
    def test_identical_strings(self):
        assert _fuzzy_overlap("abc", "abc") == 1.0

    def test_no_overlap(self):
        assert _fuzzy_overlap("abc", "xyz") == 0.0

    def test_partial_overlap(self):
        # {a,b,c} & {a,b,d} = {a,b} → 2 / max(3,3) = 0.666...
        result = _fuzzy_overlap("abc", "abd")
        assert abs(result - 2 / 3) < 1e-9

    def test_empty_first_string(self):
        assert _fuzzy_overlap("", "abc") == 0.0

    def test_empty_second_string(self):
        assert _fuzzy_overlap("abc", "") == 0.0

    def test_both_empty(self):
        assert _fuzzy_overlap("", "") == 0.0

    def test_different_lengths(self):
        # {a,b} & {a,b,c,d} = {a,b} → 2 / max(2,4) = 0.5
        assert _fuzzy_overlap("ab", "abcd") == 0.5

    def test_duplicate_chars_in_input(self):
        # "aab" → set {a,b}, "abc" → set {a,b,c} → {a,b} / max(2,3) = 2/3
        result = _fuzzy_overlap("aab", "abc")
        assert abs(result - 2 / 3) < 1e-9


# ---------------------------------------------------------------------------
# _matches_property_filters
# ---------------------------------------------------------------------------


class TestMatchesPropertyFilters:
    def test_single_filter_matches(self):
        tags = {"domain": "hydrology", MANAGED_TAG: "true"}
        assert _matches_property_filters(tags, ["domain:hydrology"]) is True

    def test_single_filter_no_match(self):
        tags = {"domain": "hydrology"}
        assert _matches_property_filters(tags, ["domain:geology"]) is False

    def test_multiple_filters_all_match(self):
        tags = {"domain": "hydrology", "format": "iceberg", MANAGED_TAG: "true"}
        assert (
            _matches_property_filters(tags, ["domain:hydrology", "format:iceberg"])
            is True
        )

    def test_multiple_filters_partial_match(self):
        tags = {"domain": "hydrology", "format": "iceberg"}
        assert (
            _matches_property_filters(tags, ["domain:hydrology", "format:parquet"])
            is False
        )

    def test_missing_key(self):
        tags = {"domain": "hydrology"}
        assert _matches_property_filters(tags, ["region:us-east"]) is False

    def test_empty_filters_list(self):
        tags = {"domain": "hydrology"}
        assert _matches_property_filters(tags, []) is True

    def test_case_insensitive_key(self):
        tags = {"Domain": "hydrology"}
        assert _matches_property_filters(tags, ["domain:hydrology"]) is True

    def test_case_insensitive_value(self):
        tags = {"domain": "Hydrology"}
        assert _matches_property_filters(tags, ["domain:hydrology"]) is True

    def test_substring_key_match(self):
        tags = {"data_domain": "hydrology"}
        assert _matches_property_filters(tags, ["domain:hydrology"]) is True

    def test_substring_value_match(self):
        tags = {"domain": "hydrology-research"}
        assert _matches_property_filters(tags, ["domain:hydrology"]) is True

    def test_managed_tag_skipped(self):
        tags = {MANAGED_TAG: "true"}
        assert _matches_property_filters(tags, [f"{MANAGED_TAG}:true"]) is False

    def test_filter_without_colon_ignored(self):
        tags = {"domain": "hydrology"}
        assert _matches_property_filters(tags, ["no_colon_here"]) is True

    def test_filter_value_with_colon(self):
        tags = {"url": "http://example.com"}
        assert _matches_property_filters(tags, ["url:http://example.com"]) is True
