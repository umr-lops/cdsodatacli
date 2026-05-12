"""
pytest unit tests for guess_s3_path

Run with: pytest test_guess_s3_path.py -v
"""

import pytest
from cdsodatacli.s3_path import guess_s3_path  # adjust import path if needed


# ---------------------------------------------------------------------------
# S1 test cases
# ---------------------------------------------------------------------------


class TestGuessS3PathSentinel1:
    """S1 products — two path formats depending on acquisition date."""

    # Products acquired BEFORE 2023-02-20 use short product type (3 chars)
    S1_OLD_CASES = [
        (
            "S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE",
            "Sentinel-1/SAR/SLC/2019/01/18/S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE",
        ),
        (
            "S1B_IW_GRDH_1SDV_20200601T054455_20200601T054520_021937_029A2D_1B78.SAFE",
            "Sentinel-1/SAR/GRD/2020/06/01/S1B_IW_GRDH_1SDV_20200601T054455_20200601T054520_021937_029A2D_1B78.SAFE",
        ),
        (
            "S1A_WV_SLC__1SSV_20210315T120000_20210315T120015_036800_045678_ABCD.SAFE",
            "Sentinel-1/SAR/SLC/2021/03/15/S1A_WV_SLC__1SSV_20210315T120000_20210315T120015_036800_045678_ABCD.SAFE",
        ),
        (
            # Exact boundary: one day before the cutoff
            "S1A_IW_SLC__1SDV_20230219T100000_20230219T100028_047000_059ABC_1234.SAFE",
            "Sentinel-1/SAR/SLC/2023/02/19/S1A_IW_SLC__1SDV_20230219T100000_20230219T100028_047000_059ABC_1234.SAFE",
        ),
    ]

    # Products acquired ON OR AFTER 2023-02-20 use long product type (10 chars)
    S1_NEW_CASES = [
        (
            "S1A_IW_SLC__1SDV_20230220T023637_20230220T023705_047500_05ABCD_08A8.SAFE",
            "Sentinel-1/SAR/IW_SLC__1S/2023/02/20/S1A_IW_SLC__1SDV_20230220T023637_20230220T023705_047500_05ABCD_08A8.SAFE",
        ),
        (
            "S1B_IW_GRDH_1SDV_20240315T054455_20240315T054520_060000_075000_ABCD.SAFE",
            "Sentinel-1/SAR/IW_GRDH_1S/2024/03/15/S1B_IW_GRDH_1SDV_20240315T054455_20240315T054520_060000_075000_ABCD.SAFE",
        ),
        (
            "S1A_EW_GRDM_1SDH_20250701T120000_20250701T120100_058000_073000_CDEF.SAFE",
            "Sentinel-1/SAR/EW_GRDM_1S/2025/07/01/S1A_EW_GRDM_1SDH_20250701T120000_20250701T120100_058000_073000_CDEF.SAFE",
        ),
    ]

    @pytest.mark.parametrize("safename,expected", S1_OLD_CASES)
    def test_s1_old_format(self, safename, expected):
        """Pre-2023-02-20 products use 3-char product type in path."""
        assert guess_s3_path(safename) == expected

    @pytest.mark.parametrize("safename,expected", S1_NEW_CASES)
    def test_s1_new_format(self, safename, expected):
        """Post-2023-02-20 products use 10-char product type in path."""
        assert guess_s3_path(safename) == expected

    def test_s1_path_starts_with_sentinel1_sar(self):
        safename = (
            "S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE"
        )
        result = guess_s3_path(safename)
        assert result.startswith("Sentinel-1/SAR/")

    def test_s1_date_components_in_path(self):
        """Year/month/day must appear correctly in the path."""
        safename = (
            "S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE"
        )
        result = guess_s3_path(safename)
        assert "/2019/01/18/" in result

    def test_s1_safename_at_end_of_path(self):
        safename = (
            "S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE"
        )
        result = guess_s3_path(safename)
        assert result.endswith(safename)

    def test_s1_cutoff_boundary_before(self):
        """2023-02-19 is strictly before the cutoff -> old format."""
        safename = (
            "S1A_IW_SLC__1SDV_20230219T235959_20230219T235959_047000_059ABC_1234.SAFE"
        )
        result = guess_s3_path(safename)
        assert "/SLC/" in result  # short 3-char type

    def test_s1_cutoff_boundary_on(self):
        """2023-02-20 is on the cutoff -> new format."""
        safename = (
            "S1A_IW_SLC__1SDV_20230220T000000_20230220T000000_047500_05ABCD_08A8.SAFE"
        )
        result = guess_s3_path(safename)
        assert "/IW_SLC__1S/" in result  # long 10-char type


# ---------------------------------------------------------------------------
# S2 test cases
# ---------------------------------------------------------------------------


class TestGuessS3PathSentinel2:
    """S2 products — Sentinel-2/MSI/<level>/YYYY/MM/DD/"""

    S2_CASES = [
        (
            # Official ESA naming convention example
            "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE",
            "Sentinel-2/MSI/L1C/2017/01/05/S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE",
        ),
        (
            # L2A product
            "S2A_MSIL2A_20240115T235221_N0510_R130_T55HGS_20240116T021554.SAFE",
            "Sentinel-2/MSI/L2A/2024/01/15/S2A_MSIL2A_20240115T235221_N0510_R130_T55HGS_20240116T021554.SAFE",
        ),
        (
            # S2B satellite
            "S2B_MSIL1C_20221231T230409_N0509_R058_T59JKJ_20230101T001206.SAFE",
            "Sentinel-2/MSI/L1C/2022/12/31/S2B_MSIL1C_20221231T230409_N0509_R058_T59JKJ_20230101T001206.SAFE",
        ),
        (
            # S2C satellite (newer)
            "S2C_MSIL1C_20260511T113321_N0512_R080_T29SNC_20260511T150841.SAFE",
            "Sentinel-2/MSI/L1C/2026/05/11/S2C_MSIL1C_20260511T113321_N0512_R080_T29SNC_20260511T150841.SAFE",
        ),
        (
            # L2A with different tile and date
            "S2B_MSIL2A_20221231T230409_N0509_R058_T59JKJ_20230101T001206.SAFE",
            "Sentinel-2/MSI/L2A/2022/12/31/S2B_MSIL2A_20221231T230409_N0509_R058_T59JKJ_20230101T001206.SAFE",
        ),
    ]

    @pytest.mark.parametrize("safename,expected", S2_CASES)
    def test_s2_path(self, safename, expected):
        assert guess_s3_path(safename) == expected

    def test_s2_path_starts_with_sentinel2_msi(self):
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        result = guess_s3_path(safename)
        assert result.startswith("Sentinel-2/MSI/")

    def test_s2_not_optical(self):
        """rad_path must be Sentinel-2/MSI/, NOT Sentinel-2/OPTICAL/."""
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        result = guess_s3_path(safename)
        assert "OPTICAL" not in result

    def test_s2_l1c_level_in_path(self):
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        assert "/L1C/" in guess_s3_path(safename)

    def test_s2_l2a_level_in_path(self):
        safename = "S2A_MSIL2A_20240115T235221_N0510_R130_T55HGS_20240116T021554.SAFE"
        assert "/L2A/" in guess_s3_path(safename)

    def test_s2_date_components_in_path(self):
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        result = guess_s3_path(safename)
        assert "/2017/01/05/" in result

    def test_s2_safename_at_end_of_path(self):
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        result = guess_s3_path(safename)
        assert result.endswith(safename)

    def test_s2_discriminator_date_not_used_for_path(self):
        """The path date must come from field[2] (sensing start), not field[6] (discriminator)."""
        # sensing start: 20170105, discriminator: 20170105T013443 (same day here but different time)
        safename = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE"
        result = guess_s3_path(safename)
        # path date should be from sensing start 20170105
        assert "/2017/01/05/" in result


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestGuessS3PathErrors:
    """Unsupported or malformed inputs."""

    def test_unsupported_mission_raises(self):
        with pytest.raises(ValueError):
            guess_s3_path("S3A_OL_1_EFR____20200101T000000_20200101T000024_ABCD.SEN3")

    def test_unsupported_prefix_raises(self):
        with pytest.raises(ValueError):
            guess_s3_path("UNKNOWN_PRODUCT_20200101.SAFE")

    def test_empty_string_raises(self):
        with pytest.raises((ValueError, IndexError)):
            guess_s3_path("")


# ---------------------------------------------------------------------------
# Path structure invariants (parametrized across both missions)
# ---------------------------------------------------------------------------

ALL_CASES = [
    "S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE",
    "S1B_IW_GRDH_1SDV_20240315T054455_20240315T054520_060000_075000_ABCD.SAFE",
    "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE",
    "S2B_MSIL2A_20221231T230409_N0509_R058_T59JKJ_20230101T001206.SAFE",
]


class TestGuessS3PathInvariants:
    """Properties that must hold for every supported product."""

    @pytest.mark.parametrize("safename", ALL_CASES)
    def test_ends_with_safename(self, safename):
        assert guess_s3_path(safename).endswith(safename)

    @pytest.mark.parametrize("safename", ALL_CASES)
    def test_no_leading_slash(self, safename):
        assert not guess_s3_path(safename).startswith("/")

    @pytest.mark.parametrize("safename", ALL_CASES)
    def test_no_double_slash(self, safename):
        assert "//" not in guess_s3_path(safename)

    @pytest.mark.parametrize("safename", ALL_CASES)
    def test_contains_year_month_day_segments(self, safename):
        """Path must contain 4-digit/2-digit/2-digit date segments."""
        import re

        result = guess_s3_path(safename)
        assert re.search(
            r"/\d{4}/\d{2}/\d{2}/", result
        ), f"No YYYY/MM/DD pattern found in: {result}"

    @pytest.mark.parametrize("safename", ALL_CASES)
    def test_starts_with_correct_mission_prefix(self, safename):
        result = guess_s3_path(safename)
        if safename.startswith("S1"):
            assert result.startswith("Sentinel-1/")
        elif safename.startswith("S2"):
            assert result.startswith("Sentinel-2/")
