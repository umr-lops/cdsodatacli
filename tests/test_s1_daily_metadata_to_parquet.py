#!/usr/bin/env python3
"""
Unit tests for s1_daily_metadata_to_parquet.py.

Tests cover:
- Date validation
- File skipping (--overwrite flag)
- Parquet saving with complex columns
- Main function orchestration
"""

import datetime
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Polygon

from cdsodatacli.scripts.s1_daily_metadata_to_parquet import (
    WV_IMAGETTES_COLUMN,
    S1_MODES_TO_KEEP,
    add_wv_imagettes_column,
    convert_complex_columns_to_string,
    create_query_gdf,
    enrich_existing_parquet,
    extract_operational_mode,
    filter_by_operational_mode,
    generate_daily_dates,
    get_wv_imagettes_json,
    get_parquet_filepath,
    is_wv_imagette_product,
    parse_args,
    parse_imagette_names,
    process_day,
    save_to_parquet,
    validate_dates,
)


class TestDateValidation:
    """Tests for date validation functions."""

    def test_validate_dates_valid(self):
        """Test valid date range."""
        start, stop = validate_dates("2026-01-01", "2026-01-31")
        assert start == datetime.date(2026, 1, 1)
        assert stop == datetime.date(2026, 1, 31)

    def test_validate_dates_invalid_format(self):
        """Test invalid date format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_dates("2026/01/01", "2026-01-31")

    def test_validate_dates_start_after_stop(self):
        """Test start date after stop date raises ValueError."""
        with pytest.raises(ValueError, match="must be before"):
            validate_dates("2026-01-31", "2026-01-01")

    def test_validate_dates_future_date(self, caplog):
        """Test future date is limited to today."""
        import logging

        caplog.set_level(logging.WARNING)

        future = (datetime.date.today() + datetime.timedelta(days=365)).isoformat()
        start = datetime.date.today().isoformat()
        start_dt, stop_dt = validate_dates(start, future)

        assert stop_dt <= datetime.date.today()
        assert "future" in caplog.text.lower()

    def test_generate_daily_dates(self):
        """Test daily date generation."""
        start = datetime.date(2026, 1, 1)
        stop = datetime.date(2026, 1, 5)
        dates = generate_daily_dates(start, stop)
        assert len(dates) == 5
        assert dates[0] == start
        assert dates[-1] == stop


class TestQueryCreation:
    """Tests for query GeoDataFrame creation."""

    def test_create_query_gdf(self):
        """Test creation of query GeoDataFrame."""
        date = datetime.date(2026, 1, 15)
        gdf = create_query_gdf(date)

        assert len(gdf) == 1
        assert "start_datetime" in gdf.columns
        assert "end_datetime" in gdf.columns
        assert "geometry" in gdf.columns
        assert "collection" in gdf.columns
        assert gdf["collection"].iloc[0] == "SENTINEL-1"
        assert "id_query" in gdf.columns
        assert gdf["id_query"].iloc[0] == "s1_daily_20260115"

    def test_create_query_gdf_geometry(self):
        """Test that query has world polygon geometry."""
        date = datetime.date(2026, 1, 15)
        gdf = create_query_gdf(date)

        geom = gdf["geometry"].iloc[0]
        assert geom is not None
        assert geom.geom_type == "Polygon"
        # Should be the world polygon (-180 to 180)
        assert geom.bounds[0] <= -180
        assert geom.bounds[2] >= 180


class TestFileHandling:
    """Tests for file handling and --overwrite flag."""

    def test_get_parquet_filepath(self):
        """Test parquet file path generation."""
        date = datetime.date(2026, 1, 15)
        output_dir = Path("/tmp/test")
        filepath = get_parquet_filepath(date, output_dir)
        assert filepath == output_dir / "s1_2026-01-15.parquet"

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_skip_existing(self, mock_fetch, tmp_path):
        """Test that process_day skips existing file when overwrite=False."""
        date = datetime.date(2026, 1, 15)
        filepath = get_parquet_filepath(date, tmp_path)
        filepath.touch()

        result_date, num_products, elapsed = process_day(
            date=date,
            output_dir=tmp_path,
            overwrite=False,
        )

        assert result_date == date
        assert num_products == -2  # Skipped
        mock_fetch.assert_not_called()

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_overwrite_existing(self, mock_fetch, tmp_path):
        """Test that process_day overwrites existing file when overwrite=True."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Name": ["name1", "name2"],
            }
        )

        date = datetime.date(2026, 1, 15)
        filepath = get_parquet_filepath(date, tmp_path)
        filepath.touch()

        result_date, num_products, elapsed = process_day(
            date=date,
            output_dir=tmp_path,
            overwrite=True,
        )

        assert result_date == date
        assert num_products == 2
        mock_fetch.assert_called_once()

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_no_results(self, mock_fetch, tmp_path):
        """Test process_day when no products are found."""
        mock_fetch.return_value = pd.DataFrame()

        date = datetime.date(2026, 1, 15)
        result_date, num_products, elapsed = process_day(
            date=date,
            output_dir=tmp_path,
        )

        assert result_date == date
        assert num_products == 0
        mock_fetch.assert_called_once()

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_error(self, mock_fetch, tmp_path):
        """Test process_day when fetch_data raises an exception."""
        mock_fetch.side_effect = Exception("Network error")

        date = datetime.date(2026, 1, 15)
        result_date, num_products, elapsed = process_day(
            date=date,
            output_dir=tmp_path,
        )

        assert result_date == date
        assert num_products == -1  # Error


class TestModeFilter:
    """Tests for filtering products by operational mode (issue #150)."""

    @staticmethod
    def _attrs(mode):
        if mode is None:
            return None
        return [
            {"Name": "origin", "Value": "ESA", "ValueType": "String"},
            {"Name": "operationalMode", "Value": mode, "ValueType": "String"},
        ]

    def test_modes_to_keep_constant(self):
        """The modes to keep match issue #150 requirements."""
        assert set(S1_MODES_TO_KEEP) == {
            "WV",
            "IW",
            "EW",
            "S1",
            "S2",
            "S3",
            "S4",
            "S5",
            "S6",
        }
        for dropped in ["N1", "N2", "N3", "N4", "N5", "N6", "EN", "AI"]:
            assert dropped not in S1_MODES_TO_KEEP

    def test_extract_operational_mode_list_of_dicts(self):
        assert extract_operational_mode(self._attrs("IW")) == "IW"
        assert extract_operational_mode(self._attrs("N1")) == "N1"

    def test_extract_operational_mode_json_string(self):
        assert extract_operational_mode(json.dumps(self._attrs("S1"))) == "S1"

    def test_extract_operational_mode_missing_or_invalid(self):
        assert extract_operational_mode(None) is None
        assert extract_operational_mode([]) is None
        assert extract_operational_mode([{"Name": "origin", "Value": "ESA"}]) is None
        assert extract_operational_mode("not json") is None
        assert extract_operational_mode("plain string") is None

    def test_filter_keeps_wanted_modes(self):
        df = pd.DataFrame(
            {
                "Id": [f"id{i}" for i in range(len(S1_MODES_TO_KEEP))],
                "Attributes": [self._attrs(m) for m in S1_MODES_TO_KEEP],
            }
        )
        df_filtered = filter_by_operational_mode(df)
        assert len(df_filtered) == len(S1_MODES_TO_KEEP)
        assert set(df_filtered["Id"]) == set(df["Id"])

    def test_filter_drops_unwanted_modes(self):
        kept = ["IW", "S1", "WV"]
        dropped = ["N1", "N2", "N3", "N4", "N5", "N6", "EN", "AI"]
        df = pd.DataFrame(
            {
                "Id": [f"id{i}" for i in range(len(kept) + len(dropped))],
                "Attributes": [self._attrs(m) for m in kept + dropped],
            }
        )
        df_filtered = filter_by_operational_mode(df)
        assert len(df_filtered) == len(kept)
        modes = df_filtered["Attributes"].apply(extract_operational_mode)
        assert set(modes) == set(kept)

    def test_filter_drops_rows_without_mode(self):
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Attributes": [self._attrs("IW"), None],
            }
        )
        df_filtered = filter_by_operational_mode(df)
        assert list(df_filtered["Id"]) == ["id1"]

    def test_filter_custom_modes_to_keep(self):
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Attributes": [self._attrs("IW"), self._attrs("WV")],
            }
        )
        df_filtered = filter_by_operational_mode(df, modes_to_keep=["WV"])
        assert list(df_filtered["Id"]) == ["id2"]

    def test_filter_handles_string_attributes(self):
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Attributes": [
                    json.dumps(self._attrs("IW")),
                    json.dumps(self._attrs("EN")),
                ],
            }
        )
        df_filtered = filter_by_operational_mode(df)
        assert list(df_filtered["Id"]) == ["id1"]

    def test_filter_empty_or_missing_attributes_column(self):
        empty = pd.DataFrame()
        assert filter_by_operational_mode(empty).empty
        df_no_attrs = pd.DataFrame({"Id": ["id1"]})
        assert len(filter_by_operational_mode(df_no_attrs)) == 1

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_filters_modes(self, mock_fetch, tmp_path):
        """process_day must drop products not in kept modes before saving."""
        kept = ["IW", "S1", "WV", "EN"]
        mock_fetch.return_value = pd.DataFrame(
            {
                "Id": [f"id{i}" for i in range(len(kept))],
                "Name": [f"name{i}" for i in range(len(kept))],
                "Attributes": [self._attrs(m) for m in kept],
            }
        )

        date = datetime.date(2026, 1, 15)
        _, num_products, _ = process_day(date=date, output_dir=tmp_path)

        assert num_products == 3  # EN dropped
        filepath = get_parquet_filepath(date, tmp_path)
        df_loaded = pd.read_parquet(filepath)
        assert len(df_loaded) == 3

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_all_dropped(self, mock_fetch, tmp_path):
        """process_day returns 0 products when all modes are dropped."""
        mock_fetch.return_value = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Name": ["name1", "name2"],
                "Attributes": [self._attrs("N1"), self._attrs("AI")],
            }
        )

        date = datetime.date(2026, 1, 15)
        _, num_products, _ = process_day(date=date, output_dir=tmp_path)

        assert num_products == 0
        assert not get_parquet_filepath(date, tmp_path).exists()


class TestComplexColumnsConversion:
    """Tests for complex column conversion to strings."""

    def test_convert_geometry_to_wkt(self):
        """Test conversion of geometry column to WKT."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "geometry": [polygon],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        # The conversion should either drop 'geometry' and create 'geometry_wkt'
        # OR convert 'geometry' to string
        if "geometry_wkt" in df_converted.columns:
            assert "geometry" not in df_converted.columns
            assert df_converted["geometry_wkt"].iloc[0] == polygon.wkt
        else:
            # Fallback: geometry converted to string
            assert "geometry" in df_converted.columns
            assert isinstance(df_converted["geometry"].iloc[0], str)

    def test_convert_geofootprint_column(self):
        """Test conversion of GeoFootprint column."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "GeoFootprint": [polygon],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        # The conversion should either drop 'GeoFootprint' and create 'GeoFootprint_wkt'
        # OR convert 'GeoFootprint' to string
        if "GeoFootprint_wkt" in df_converted.columns:
            assert "GeoFootprint" not in df_converted.columns
            assert df_converted["GeoFootprint_wkt"].iloc[0] == polygon.wkt
        else:
            # Fallback: GeoFootprint converted to string
            assert "GeoFootprint" in df_converted.columns
            assert isinstance(df_converted["GeoFootprint"].iloc[0], str)

    def test_convert_checksum_column(self):
        """Test conversion of Checksum column (list of dicts)."""
        checksum = [{"Value": "abc123", "Algorithm": "MD5"}]
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "Checksum": [checksum],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        assert "Checksum" in df_converted.columns
        # json.dumps() produces double quotes, so json.loads() should work
        parsed = json.loads(df_converted["Checksum"].iloc[0])
        assert parsed == checksum

    def test_convert_contentdate_column(self):
        """Test conversion of ContentDate column (dict with Start/End)."""
        content_date = {"Start": "2026-01-01T00:00:00Z", "End": "2026-01-01T01:00:00Z"}
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "ContentDate": [content_date],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        # The conversion should either create separate columns or convert to string
        if "ContentDate_start" in df_converted.columns:
            assert "ContentDate" not in df_converted.columns
            assert df_converted["ContentDate_start"].iloc[0] == content_date["Start"]
            assert df_converted["ContentDate_end"].iloc[0] == content_date["End"]
        else:
            # Fallback: ContentDate converted to string (JSON)
            assert "ContentDate" in df_converted.columns
            assert isinstance(df_converted["ContentDate"].iloc[0], str)

    def test_convert_list_column(self):
        """Test conversion of column containing lists."""
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Tags": [["tag1", "tag2"], ["tag3"]],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        assert "Tags" in df_converted.columns
        # In pandas 3.0, string columns have dtype 'str'
        # In pandas 2.x, they have dtype 'object'
        dtype = str(df_converted["Tags"].dtype)
        # Accept both 'object' (pandas 2.x) and 'str' or 'string' (pandas 3.x)
        acceptable_dtypes = ["object", "str", "string"]
        assert dtype in acceptable_dtypes
        # Check that the value is a string (not a list)
        # json.dumps() produces JSON string like '["tag1", "tag2"]'
        assert isinstance(df_converted["Tags"].iloc[0], str)
        # Should be valid JSON
        parsed = json.loads(df_converted["Tags"].iloc[0])
        assert parsed == ["tag1", "tag2"]

    def test_convert_numpy_array_column(self):
        """Test conversion of column containing numpy arrays."""
        arr = np.array([1, 2, 3])
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "Values": [arr],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        assert "Values" in df_converted.columns
        assert isinstance(df_converted["Values"].iloc[0], str)

    def test_convert_mixed_types(self):
        """Test conversion of column with mixed types."""
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Mixed": ["string", ["list", "items"]],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        assert "Mixed" in df_converted.columns
        # Both should be strings
        assert isinstance(df_converted["Mixed"].iloc[0], str)
        assert isinstance(df_converted["Mixed"].iloc[1], str)
        # The list should be converted to JSON string
        parsed = json.loads(df_converted["Mixed"].iloc[1])
        assert parsed == ["list", "items"]

    def test_convert_complex_attributes(self):
        """Test conversion of Attributes column (list of dicts)."""
        attributes = [
            {"Name": "operationalMode", "Value": "IW"},
            {"Name": "productType", "Value": "GRD"},
        ]
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "Attributes": [attributes],
            }
        )

        df_converted = convert_complex_columns_to_string(df)

        assert "Attributes" in df_converted.columns
        # Should be valid JSON
        parsed = json.loads(df_converted["Attributes"].iloc[0])
        assert parsed == attributes


class TestSaveToParquet:
    """Tests for parquet saving."""

    def test_save_to_parquet_simple(self, tmp_path):
        """Test saving simple DataFrame to parquet."""
        df = pd.DataFrame(
            {
                "Id": ["id1", "id2"],
                "Name": ["name1", "name2"],
            }
        )

        date = datetime.date(2026, 1, 15)
        filepath = save_to_parquet(df, date, tmp_path)

        assert filepath.exists()
        df_loaded = pd.read_parquet(filepath)
        assert len(df_loaded) == 2
        assert "Id" in df_loaded.columns
        assert "Name" in df_loaded.columns

    def test_save_to_parquet_with_geometry(self, tmp_path):
        """Test saving DataFrame with geometry column."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "geometry": [polygon],
            }
        )

        date = datetime.date(2026, 1, 15)
        filepath = save_to_parquet(df, date, tmp_path)

        assert filepath.exists()
        df_loaded = pd.read_parquet(filepath)
        # Either geometry_wkt was created or geometry was converted to string
        if "geometry_wkt" in df_loaded.columns:
            assert df_loaded["geometry_wkt"].iloc[0] == polygon.wkt
        else:
            assert "geometry" in df_loaded.columns
            assert isinstance(df_loaded["geometry"].iloc[0], str)

    def test_save_to_parquet_with_complex_columns(self, tmp_path):
        """Test saving DataFrame with complex columns."""
        checksum = [{"Value": "abc123", "Algorithm": "MD5"}]
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "Checksum": [checksum],
                "Tags": [["tag1", "tag2"]],
            }
        )

        date = datetime.date(2026, 1, 15)
        filepath = save_to_parquet(df, date, tmp_path)

        assert filepath.exists()
        df_loaded = pd.read_parquet(filepath)
        # json.dumps() produces double quotes, so json.loads() should work
        parsed = json.loads(df_loaded["Checksum"].iloc[0])
        assert parsed == checksum
        assert "Tags" in df_loaded.columns
        assert isinstance(df_loaded["Tags"].iloc[0], str)
        # Tags should be valid JSON
        parsed_tags = json.loads(df_loaded["Tags"].iloc[0])
        assert parsed_tags == ["tag1", "tag2"]

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.logger")
    def test_save_to_parquet_fallback(self, mock_logger, tmp_path):
        """Test fallback when pyarrow fails."""
        df = pd.DataFrame(
            {
                "Id": ["id1"],
                "Data": [{"nested": "data"}],
            }
        )

        # Mock pyarrow to fail, then fastparquet to work
        with patch("pandas.DataFrame.to_parquet") as mock_to_parquet:
            # First call fails, second call succeeds
            mock_to_parquet.side_effect = [
                Exception("PyArrow error"),
                None,
            ]

            date = datetime.date(2026, 1, 15)
            save_to_parquet(df, date, tmp_path)

            # Should have called to_parquet twice (pyarrow then fastparquet)
            assert mock_to_parquet.call_count == 2


class TestMainFunction:
    """Tests for main function orchestration."""

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.setup_logging")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.validate_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.generate_daily_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.process_day")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.get_credentials")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_happy_path(
        self,
        mock_parse_args,
        mock_get_credentials,
        mock_process_day,
        mock_generate_dates,
        mock_validate_dates,
        mock_setup_logging,
        tmp_path,
    ):
        """Test main function with happy path."""
        # Setup mocks
        mock_args = MagicMock()
        mock_args.start_date = "2026-01-01"
        mock_args.stop_date = "2026-01-03"
        mock_args.outputdir = str(tmp_path)
        mock_args.config_file = None
        mock_args.log_level = "INFO"
        mock_args.email = None
        mock_args.password = None
        mock_args.cache_dir = None
        mock_args.checkpoint_dir = None
        mock_args.batch_size = 7
        mock_args.querymode = "seq"
        mock_args.max_workers = 4
        mock_args.overwrite = False
        mock_parse_args.return_value = mock_args

        mock_validate_dates.return_value = (
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 3),
        )
        mock_generate_dates.return_value = [
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 2),
            datetime.date(2026, 1, 3),
        ]
        mock_process_day.side_effect = [
            (datetime.date(2026, 1, 1), 100, 10.0),
            (datetime.date(2026, 1, 2), 150, 12.0),
            (datetime.date(2026, 1, 3), 200, 15.0),
        ]
        mock_get_credentials.return_value = (None, None)

        # Import and run main
        from cdsodatacli.scripts.s1_daily_metadata_to_parquet import main

        with patch("sys.exit") as mock_exit:
            main()
            mock_exit.assert_not_called()

        # Verify process_day was called 3 times
        assert mock_process_day.call_count == 3

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.setup_logging")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.validate_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.generate_daily_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.process_day")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.get_credentials")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_with_skip_and_fail(
        self,
        mock_parse_args,
        mock_get_credentials,
        mock_process_day,
        mock_generate_dates,
        mock_validate_dates,
        mock_setup_logging,
        tmp_path,
    ):
        """Test main with skipped and failed days."""
        mock_args = MagicMock()
        mock_args.start_date = "2026-01-01"
        mock_args.stop_date = "2026-01-03"
        mock_args.outputdir = str(tmp_path)
        mock_args.config_file = None
        mock_args.log_level = "INFO"
        mock_args.email = None
        mock_args.password = None
        mock_args.cache_dir = None
        mock_args.checkpoint_dir = None
        mock_args.batch_size = 7
        mock_args.querymode = "seq"
        mock_args.max_workers = 4
        mock_args.overwrite = False
        mock_parse_args.return_value = mock_args

        mock_validate_dates.return_value = (
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 3),
        )
        mock_generate_dates.return_value = [
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 2),
            datetime.date(2026, 1, 3),
        ]
        mock_process_day.side_effect = [
            (datetime.date(2026, 1, 1), -2, 0.0),  # Skipped
            (datetime.date(2026, 1, 2), -1, 0.0),  # Failed
            (datetime.date(2026, 1, 3), 200, 15.0),  # Success
        ]
        mock_get_credentials.return_value = (None, None)

        from cdsodatacli.scripts.s1_daily_metadata_to_parquet import main

        with patch("sys.exit") as mock_exit:
            main()
            mock_exit.assert_not_called()

        assert mock_process_day.call_count == 3

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.setup_logging")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.validate_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.generate_daily_dates")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.process_day")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.get_credentials")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_counts_failed_days(
        self,
        mock_parse_args,
        mock_get_credentials,
        mock_process_day,
        mock_generate_dates,
        mock_validate_dates,
        mock_setup_logging,
        tmp_path,
        caplog,
    ):
        """Days that raise/fail in process_day must be counted in the summary."""
        import logging

        caplog.set_level(logging.INFO)

        mock_args = MagicMock()
        mock_args.start_date = "2026-01-01"
        mock_args.stop_date = "2026-01-03"
        mock_args.outputdir = str(tmp_path)
        mock_args.config_file = None
        mock_args.log_level = "INFO"
        mock_args.email = None
        mock_args.password = None
        mock_args.cache_dir = None
        mock_args.checkpoint_dir = None
        mock_args.batch_size = 7
        mock_args.querymode = "seq"
        mock_args.max_workers = 4
        mock_args.overwrite = False
        mock_parse_args.return_value = mock_args

        mock_validate_dates.return_value = (
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 3),
        )
        mock_generate_dates.return_value = [
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 2),
            datetime.date(2026, 1, 3),
        ]
        mock_process_day.side_effect = [
            (datetime.date(2026, 1, 1), -1, 0.0),  # Failed
            (datetime.date(2026, 1, 2), -1, 0.0),  # Failed
            (datetime.date(2026, 1, 3), 200, 15.0),  # Success
        ]
        mock_get_credentials.return_value = (None, None)

        from cdsodatacli.scripts.s1_daily_metadata_to_parquet import main

        with patch("sys.exit") as mock_exit:
            main()
            mock_exit.assert_not_called()

        assert "Days failed: 2" in caplog.text

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.setup_logging")
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.validate_dates")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_invalid_dates(
        self,
        mock_parse_args,
        mock_validate_dates,
        mock_setup_logging,
    ):
        """Test main with invalid dates."""
        mock_args = MagicMock()
        mock_args.start_date = "invalid"
        mock_args.stop_date = "2026-01-31"
        mock_args.outputdir = "/tmp"
        mock_args.config_file = None
        mock_args.log_level = "INFO"
        mock_args.email = None
        mock_args.password = None
        mock_args.cache_dir = None
        mock_args.checkpoint_dir = None
        mock_args.batch_size = 7
        mock_args.querymode = "seq"
        mock_args.max_workers = 4
        mock_args.overwrite = False
        mock_parse_args.return_value = mock_args

        mock_validate_dates.side_effect = ValueError("Invalid date format")

        from cdsodatacli.scripts.s1_daily_metadata_to_parquet import main

        with patch("sys.exit") as mock_exit:
            main()
            mock_exit.assert_called_once_with(1)


class TestArgumentParsing:
    """Tests for argument parsing."""

    def test_parse_args_minimal(self):
        """Test minimal required arguments."""
        test_args = [
            "--start-date",
            "2026-01-01",
            "--stop-date",
            "2026-01-31",
            "--outputdir",
            "/tmp/test",
        ]
        with patch("sys.argv", ["script"] + test_args):
            args = parse_args()
            assert args.start_date == "2026-01-01"
            assert args.stop_date == "2026-01-31"
            assert args.outputdir == "/tmp/test"
            assert args.overwrite is False
            assert args.log_level == "INFO"

    def test_parse_args_with_overwrite(self):
        """Test with overwrite flag."""
        test_args = [
            "--start-date",
            "2026-01-01",
            "--stop-date",
            "2026-01-31",
            "--outputdir",
            "/tmp/test",
            "--overwrite",
        ]
        with patch("sys.argv", ["script"] + test_args):
            args = parse_args()
            assert args.overwrite is True

    def test_parse_args_with_all_options(self):
        """Test with all options."""
        test_args = [
            "--start-date",
            "2026-01-01",
            "--stop-date",
            "2026-01-31",
            "--outputdir",
            "/tmp/test",
            "--config-file",
            "/tmp/config.yml",
            "--log-level",
            "DEBUG",
            "--email",
            "user@test.com",
            "--password",
            "pass",
            "--cache-dir",
            "/tmp/cache",
            "--checkpoint-dir",
            "/tmp/ckpt",
            "--batch-size",
            "3",
            "--querymode",
            "multi",
            "--max-workers",
            "8",
            "--overwrite",
        ]
        with patch("sys.argv", ["script"] + test_args):
            args = parse_args()
            assert args.start_date == "2026-01-01"
            assert args.stop_date == "2026-01-31"
            assert args.outputdir == "/tmp/test"
            assert args.config_file == "/tmp/config.yml"
            assert args.log_level == "DEBUG"
            assert args.email == "user@test.com"
            assert args.password == "pass"
            assert args.cache_dir == "/tmp/cache"
            assert args.checkpoint_dir == "/tmp/ckpt"
            assert args.batch_size == 3
            assert args.querymode == "multi"
            assert args.max_workers == 8
            assert args.overwrite is True


OCN_SAFE = "S1A_WV_OCN__2SSV_20240918T060833_20240918T064359_055715_06CE05_E53B.SAFE"
SLC_SAFE = "S1A_WV_SLC__1SSV_20240918T004251_20240918T004802_055712_06CDE7_2919.SAFE"
RAW_SAFE = "S1A_WV_RAW__0SSV_20240918T060833_20240918T064359_055715_06CE05_AAAA.SAFE"
IW_SAFE = "S1A_IW_SLC__1SDV_20240918T060833_20240918T064359_055715_06CE05_BBBB.SAFE"
NAME_1 = "s1a-wv1-ocn-vv-20240918t060833-20240918t060836-055715-06ce05-001.nc"
NAME_2 = "s1a-wv2-ocn-vv-20240918t060847-20240918t060850-055715-06ce05-002.nc"
NAME_SLC = "s1a-wv1-slc-vv-20240918t004616-20240918t004619-055712-06cde7-015.tiff"
FETCH_NAMES = "cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_imagette_names"


class TestImagettes:
    """Tests for the imagette listing of WV OCN and WV SLC products."""

    def test_is_wv_imagette_product(self):
        assert is_wv_imagette_product(OCN_SAFE)
        assert is_wv_imagette_product(SLC_SAFE)
        assert not is_wv_imagette_product(RAW_SAFE)
        assert not is_wv_imagette_product(IW_SAFE)
        assert not is_wv_imagette_product(None)

    def test_parse_imagette_names_sorted_by_index(self):
        records = parse_imagette_names([NAME_2, NAME_1])
        assert [r["index"] for r in records] == [1, 2]
        assert records[0]["Name"] == NAME_1
        assert records[0]["ContentDate"] == {
            "Start": "2024-09-18T06:08:33Z",
            "End": "2024-09-18T06:08:36Z",
        }

    def test_parse_imagette_names_slc(self):
        record = parse_imagette_names([NAME_SLC])[0]
        assert record["index"] == 15
        assert record["ContentDate"]["Start"] == "2024-09-18T00:46:16Z"

    def test_parse_imagette_names_unexpected_name(self):
        with pytest.raises(ValueError, match="Unexpected"):
            parse_imagette_names(["manifest.safe"])

    def test_parse_imagette_names_duplicate_index(self):
        other = NAME_2.replace("-002.nc", "-001.nc")
        with pytest.raises(ValueError, match="Duplicate"):
            parse_imagette_names([NAME_1, other])

    @patch(FETCH_NAMES, return_value=[NAME_2, NAME_1])
    def test_get_wv_imagettes_json(self, mock_names):
        value = get_wv_imagettes_json("id1", OCN_SAFE)
        assert [r["index"] for r in json.loads(value)] == [1, 2]

    @patch(FETCH_NAMES, side_effect=Exception("HTTP 429"))
    def test_get_wv_imagettes_json_returns_none_on_error(self, mock_names):
        assert get_wv_imagettes_json("id1", OCN_SAFE) is None

    @patch(FETCH_NAMES, return_value=[NAME_1])
    def test_add_wv_imagettes_column_only_for_wv(self, mock_names):
        df = pd.DataFrame(
            {"Id": ["a", "b", "c"], "Name": [OCN_SAFE, RAW_SAFE, IW_SAFE]},
            index=[10, 20, 30],
        )
        out = add_wv_imagettes_column(df)
        assert out[WV_IMAGETTES_COLUMN].notna().tolist() == [True, False, False]
        assert mock_names.call_count == 1
        assert WV_IMAGETTES_COLUMN not in df.columns  # input untouched

    @patch(FETCH_NAMES, return_value=[NAME_1])
    def test_add_wv_imagettes_column_skips_rows_already_done(self, mock_names):
        df = pd.DataFrame(
            {
                "Id": ["a", "b"],
                "Name": [OCN_SAFE, SLC_SAFE],
                WV_IMAGETTES_COLUMN: ["[]", None],
            }
        )
        out = add_wv_imagettes_column(df)
        assert mock_names.call_count == 1
        assert out[WV_IMAGETTES_COLUMN].iloc[0] == "[]"
        assert out[WV_IMAGETTES_COLUMN].iloc[1] is not None

    @patch(FETCH_NAMES, side_effect=[[NAME_1], Exception("boom")])
    def test_add_wv_imagettes_column_keeps_going_after_a_failure(self, mock_names):
        df = pd.DataFrame({"Id": ["a", "b"], "Name": [OCN_SAFE, SLC_SAFE]})
        out = add_wv_imagettes_column(df, max_workers=1)
        assert out[WV_IMAGETTES_COLUMN].notna().tolist() == [True, False]

    @patch(FETCH_NAMES, return_value=[NAME_1])
    def test_enrich_existing_parquet(self, mock_names, tmp_path):
        filepath = tmp_path / "s1_2026-01-15.parquet"
        pd.DataFrame({"Id": ["a", "b"], "Name": [OCN_SAFE, RAW_SAFE]}).to_parquet(
            filepath
        )
        assert enrich_existing_parquet(filepath) == 2
        df = pd.read_parquet(filepath)
        assert json.loads(df[WV_IMAGETTES_COLUMN].iloc[0])[0]["Name"] == NAME_1
        assert pd.isna(df[WV_IMAGETTES_COLUMN].iloc[1])
        assert not list(tmp_path.glob("*.tmp"))
        # a second pass has nothing left to query
        mock_names.reset_mock()
        assert enrich_existing_parquet(filepath) is None
        mock_names.assert_not_called()

    @patch(FETCH_NAMES, return_value=[NAME_1])
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_with_imagettes(self, mock_fetch, mock_names, tmp_path):
        attrs = [{"Name": "operationalMode", "Value": "WV"}]
        mock_fetch.return_value = pd.DataFrame(
            {"Id": ["a"], "Name": [OCN_SAFE], "Attributes": [attrs]}
        )
        date = datetime.date(2026, 1, 15)
        _, num_products, _ = process_day(
            date=date, output_dir=tmp_path, wv_imagettes=True
        )
        assert num_products == 1
        df = pd.read_parquet(get_parquet_filepath(date, tmp_path))
        assert WV_IMAGETTES_COLUMN in df.columns

    @patch(FETCH_NAMES, return_value=[NAME_1])
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_without_flag_adds_no_column(
        self, mock_fetch, mock_names, tmp_path
    ):
        attrs = [{"Name": "operationalMode", "Value": "WV"}]
        mock_fetch.return_value = pd.DataFrame(
            {"Id": ["a"], "Name": [OCN_SAFE], "Attributes": [attrs]}
        )
        date = datetime.date(2026, 1, 15)
        process_day(date=date, output_dir=tmp_path)
        df = pd.read_parquet(get_parquet_filepath(date, tmp_path))
        assert WV_IMAGETTES_COLUMN not in df.columns
        mock_names.assert_not_called()

    @patch(FETCH_NAMES, return_value=[NAME_1])
    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.fetch_data")
    def test_process_day_existing_file_gets_the_column(
        self, mock_fetch, mock_names, tmp_path
    ):
        date = datetime.date(2026, 1, 15)
        filepath = get_parquet_filepath(date, tmp_path)
        pd.DataFrame({"Id": ["a"], "Name": [OCN_SAFE]}).to_parquet(filepath)
        _, num_products, _ = process_day(
            date=date, output_dir=tmp_path, wv_imagettes=True
        )
        assert num_products == 1
        mock_fetch.assert_not_called()  # no new catalogue query
        assert WV_IMAGETTES_COLUMN in pd.read_parquet(filepath).columns

    def test_parse_args_wv_imagettes_flag(self):
        base = [
            "script",
            "--start-date",
            "2026-01-01",
            "--stop-date",
            "2026-01-02",
            "--outputdir",
            "/tmp/x",
        ]
        with patch("sys.argv", base):
            assert parse_args().wv_imagettes is False
        with patch("sys.argv", base + ["--wv-imagettes"]):
            assert parse_args().wv_imagettes is True
