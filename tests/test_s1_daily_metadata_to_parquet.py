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
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import shapely
from shapely.geometry import Polygon

from cdsodatacli.scripts.s1_daily_metadata_to_parquet import (
    convert_complex_columns_to_string,
    create_query_gdf,
    generate_daily_dates,
    get_parquet_filepath,
    parse_args,
    process_day,
    save_to_parquet,
    setup_logging,
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
        mock_fetch.return_value = pd.DataFrame({
            "Id": ["id1", "id2"],
            "Name": ["name1", "name2"],
        })

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


class TestComplexColumnsConversion:
    """Tests for complex column conversion to strings."""

    def test_convert_geometry_to_wkt(self):
        """Test conversion of geometry column to WKT."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        df = pd.DataFrame({
            "Id": ["id1"],
            "geometry": [polygon],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "geometry" not in df_converted.columns
        assert "geometry_wkt" in df_converted.columns
        assert df_converted["geometry_wkt"].iloc[0] == polygon.wkt

    def test_convert_geofootprint_column(self):
        """Test conversion of GeoFootprint column."""
        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        df = pd.DataFrame({
            "Id": ["id1"],
            "GeoFootprint": [polygon],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "GeoFootprint" not in df_converted.columns
        assert "GeoFootprint_wkt" in df_converted.columns
        assert df_converted["GeoFootprint_wkt"].iloc[0] == polygon.wkt

    def test_convert_checksum_column(self):
        """Test conversion of Checksum column (list of dicts)."""
        checksum = [{"Value": "abc123", "Algorithm": "MD5"}]
        df = pd.DataFrame({
            "Id": ["id1"],
            "Checksum": [checksum],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "Checksum" in df_converted.columns
        assert df_converted["Checksum"].iloc[0] == json.dumps(checksum)

    def test_convert_list_column(self):
        """Test conversion of column containing lists."""
        df = pd.DataFrame({
            "Id": ["id1", "id2"],
            "Tags": [["tag1", "tag2"], ["tag3"]],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "Tags" in df_converted.columns
        assert df_converted["Tags"].dtype == object
        assert isinstance(df_converted["Tags"].iloc[0], str)

    def test_convert_numpy_array_column(self):
        """Test conversion of column containing numpy arrays."""
        arr = np.array([1, 2, 3])
        df = pd.DataFrame({
            "Id": ["id1"],
            "Values": [arr],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "Values" in df_converted.columns
        assert isinstance(df_converted["Values"].iloc[0], str)

    def test_convert_mixed_types(self):
        """Test conversion of column with mixed types."""
        df = pd.DataFrame({
            "Id": ["id1", "id2"],
            "Mixed": ["string", ["list", "items"]],
        })

        df_converted = convert_complex_columns_to_string(df)

        assert "Mixed" in df_converted.columns
        assert isinstance(df_converted["Mixed"].iloc[0], str)
        assert isinstance(df_converted["Mixed"].iloc[1], str)


class TestSaveToParquet:
    """Tests for parquet saving."""

    def test_save_to_parquet_simple(self, tmp_path):
        """Test saving simple DataFrame to parquet."""
        df = pd.DataFrame({
            "Id": ["id1", "id2"],
            "Name": ["name1", "name2"],
        })

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
        df = pd.DataFrame({
            "Id": ["id1"],
            "geometry": [polygon],
        })

        date = datetime.date(2026, 1, 15)
        filepath = save_to_parquet(df, date, tmp_path)

        assert filepath.exists()
        df_loaded = pd.read_parquet(filepath)
        assert "geometry_wkt" in df_loaded.columns
        assert df_loaded["geometry_wkt"].iloc[0] == polygon.wkt

    def test_save_to_parquet_with_complex_columns(self, tmp_path):
        """Test saving DataFrame with complex columns."""
        checksum = [{"Value": "abc123", "Algorithm": "MD5"}]
        df = pd.DataFrame({
            "Id": ["id1"],
            "Checksum": [checksum],
            "Tags": [["tag1", "tag2"]],
        })

        date = datetime.date(2026, 1, 15)
        filepath = save_to_parquet(df, date, tmp_path)

        assert filepath.exists()
        df_loaded = pd.read_parquet(filepath)
        assert df_loaded["Checksum"].iloc[0] == json.dumps(checksum)

    @patch("cdsodatacli.scripts.s1_daily_metadata_to_parquet.logger")
    def test_save_to_parquet_fallback(self, mock_logger, tmp_path):
        """Test fallback when pyarrow fails."""
        df = pd.DataFrame({
            "Id": ["id1"],
            "Data": [{"nested": "data"}],
        })

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
            "--start-date", "2026-01-01",
            "--stop-date", "2026-01-31",
            "--outputdir", "/tmp/test",
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
            "--start-date", "2026-01-01",
            "--stop-date", "2026-01-31",
            "--outputdir", "/tmp/test",
            "--overwrite",
        ]
        with patch("sys.argv", ["script"] + test_args):
            args = parse_args()
            assert args.overwrite is True

    def test_parse_args_with_all_options(self):
        """Test with all options."""
        test_args = [
            "--start-date", "2026-01-01",
            "--stop-date", "2026-01-31",
            "--outputdir", "/tmp/test",
            "--config-file", "/tmp/config.yml",
            "--log-level", "DEBUG",
            "--email", "user@test.com",
            "--password", "pass",
            "--cache-dir", "/tmp/cache",
            "--checkpoint-dir", "/tmp/ckpt",
            "--batch-size", "3",
            "--querymode", "multi",
            "--max-workers", "8",
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