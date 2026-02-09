import pytest
import pandas as pd
import os
from unittest.mock import patch
from cdsodatacli.scripts.get_ids_listing_safe_iterative import (
    add_ids_to_listing_iterative,
)


@pytest.fixture
def sample_listing(tmp_path):
    """Creates a dummy input listing file."""
    p = tmp_path / "input_listing.txt"
    safes = [
        "S1A_IW_GRDH_1SDV_20220503T000000",
        "S1A_IW_GRDH_1SDV_20220503T000001",
        "S1A_IW_GRDH_1SDV_20220503T000002",
    ]
    p.write_text("\n".join(safes))
    return str(p)


def test_add_ids_to_listing_iterative_full_success(sample_listing, tmp_path):
    """Test case where all IDs are found in the first iteration."""
    output_path = str(tmp_path / "final_output.csv")

    # Prepare mocked response from the API wrapper
    mock_df = pd.DataFrame(
        {
            "safename": [
                "S1A_IW_GRDH_1SDV_20220503T000000",
                "S1A_IW_GRDH_1SDV_20220503T000001",
                "S1A_IW_GRDH_1SDV_20220503T000002",
            ],
            "id": ["uuid0", "uuid1", "uuid2"],
        }
    )

    with patch(
        "cdsodatacli.download.add_missing_cdse_hash_ids_in_listing",
        return_value=mock_df,
    ):
        result_file = add_ids_to_listing_iterative(sample_listing, output_path)

        assert os.path.exists(result_file)
        df_out = pd.read_csv(result_file, names=["id", "safename"])
        assert len(df_out) == 3
        assert set(df_out["id"]) == {"uuid0", "uuid1", "uuid2"}


def test_add_ids_to_listing_iterative_multi_step(sample_listing, tmp_path):
    """Test case where IDs are found across multiple loops (iterations)."""
    output_path = str(tmp_path / "multi_step_output.csv")

    # 1st call returns only 2 out of 3 IDs
    mock_response_1 = pd.DataFrame(
        {
            "safename": [
                "S1A_IW_GRDH_1SDV_20220503T000000",
                "S1A_IW_GRDH_1SDV_20220503T000001",
            ],
            "id": ["uuid0", "uuid1"],
        }
    )
    # 2nd call returns the last ID
    mock_response_2 = pd.DataFrame(
        {"safename": ["S1A_IW_GRDH_1SDV_20220503T000002"], "id": ["uuid2"]}
    )

    with patch(
        "cdsodatacli.download.add_missing_cdse_hash_ids_in_listing"
    ) as mocked_api:
        mocked_api.side_effect = [mock_response_1, mock_response_2]

        result_file = add_ids_to_listing_iterative(sample_listing, output_path)

        df_out = pd.read_csv(result_file, names=["id", "safename"])
        assert len(df_out) == 3
        assert not df_out["id"].isna().any()
        assert mocked_api.call_count == 2


def test_add_ids_to_listing_no_progress_break(sample_listing, tmp_path):
    """Test that the loop breaks if no new IDs are found to avoid infinite loops."""
    output_path = str(tmp_path / "break_output.csv")

    # API consistently returns nothing
    empty_df = pd.DataFrame(columns=["safename", "id"])

    with patch(
        "cdsodatacli.download.add_missing_cdse_hash_ids_in_listing",
        return_value=empty_df,
    ):
        result_file = add_ids_to_listing_iterative(sample_listing, output_path)

        df_out = pd.read_csv(result_file, names=["id", "safename"])
        # IDs should be NaN because nothing was found, but the script should have finished
        assert df_out["id"].isna().all()


def test_add_ids_to_listing_file_not_found():
    """Test behavior when the input file does not exist."""
    with pytest.raises(FileNotFoundError):
        add_ids_to_listing_iterative("non_existent_file.txt")


def test_add_ids_to_listing_duplicates_in_api(sample_listing, tmp_path):
    """Test that the script handles duplicates returned by the API correctly."""
    output_path = str(tmp_path / "dedup_output.csv")

    # API returns the same SAFE twice with same ID
    mock_df = pd.DataFrame(
        {
            "safename": [
                "S1A_IW_GRDH_1SDV_20220503T000000",
                "S1A_IW_GRDH_1SDV_20220503T000000",
            ],
            "id": ["uuid0", "uuid0"],
        }
    )

    with patch(
        "cdsodatacli.download.add_missing_cdse_hash_ids_in_listing",
        return_value=mock_df,
    ):
        result_file = add_ids_to_listing_iterative(sample_listing, output_path)
        df_out = pd.read_csv(result_file, names=["id", "safename"])

        # Should still correspond to input length (3) even if API was weird
        assert len(df_out) == 3
