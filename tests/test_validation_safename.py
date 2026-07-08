#!/usr/bin/env python3
"""
Tests unitaires pour la validation des SAFE names.
"""

import sys
from pathlib import Path
import datetime

# Ajouter le chemin du projet
sys.path.insert(0, str(Path(__file__).parent.parent))

from cdsodatacli.get_products_metadata import (
    validate_safe_name,
    validate_safe_names,
    deduplicate_safe_names,
    detect_identifier_type,
)


class TestValidateSafeName:
    """Tests pour validate_safe_name()"""

    def test_valid_s1_standard(self):
        """Test avec un SAFE Sentinel-1 standard"""
        safe = "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        assert result["safe_name"] == safe
        assert result["mission"] == "S1A"
        assert result["mode"] == "IW"
        assert result["product_type"] == "GRDH"
        assert result["polarization"] == "DV"
        assert isinstance(result["start_timestamp"], datetime.datetime)
        assert result["start_timestamp"].strftime("%Y%m%dT%H%M%S") == "20230726T071112"
        assert result["datatake"] == "012345"
        assert result["orbit"] == "012345"

    def test_valid_s1_double_underscore(self):
        """Test avec un SAFE Sentinel-1 avec double underscore (SLC__)"""
        safe = "S1B_IW_SLC__1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        assert result["mission"] == "S1B"
        # On nettoie le underscore supplémentaire
        assert result["product_type"] == "SLC"
        assert result["polarization"] == "DV"

    def test_valid_s1_no_processing(self):
        """Test avec un SAFE Sentinel-1 sans processing ID"""
        safe = "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        # Le processing_id doit être None (pas une chaîne vide)
        assert result["processing_id"] is None

    def test_valid_s1_grvm(self):
        """Test avec GRDM (GRD Medium)"""
        safe = "S1B_IW_GRDM_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        assert result["product_type"] == "GRDM"

    def test_valid_s1_ew(self):
        """Test avec mode EW"""
        safe = "S1A_EW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        assert result["mode"] == "EW"

    def test_valid_s1_wv(self):
        """Test avec mode WV"""
        safe = "S1A_WV_SLC__1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is True
        assert result["mode"] == "WV"

    def test_invalid_mission(self):
        """Test avec mission invalide"""
        safe = "S0A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is False
        # La mission S0A n'est pas reconnue, on vérifie qu'il y a une erreur
        error_lower = result["error"].lower()
        assert (
            "mission" in error_lower
            or "missing start timestamp" in error_lower
            or "invalid" in error_lower
            or "s0a" in error_lower
        )

    def test_invalid_mode(self):
        """Test avec mode invalide"""
        safe = "S1A_XX_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is False
        assert "mode" in result["error"].lower()
        assert "XX" in result["error"]

    def test_invalid_product_type(self):
        """Test avec product type invalide"""
        safe = "S1A_IW_XXX_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        result = validate_safe_name(safe)

        assert result["is_valid"] is False
        # Vérifier une erreur liée au product type ou au format
        error_lower = result["error"].lower()
        assert (
            "product" in error_lower
            or "format" in error_lower
            or "unconverted" in error_lower
            or "xxx" in error_lower
        )

    def test_empty_name(self):
        """Test avec nom vide"""
        result = validate_safe_name("")
        assert result["is_valid"] is False
        assert "empty" in result["error"].lower()

    def test_too_short(self):
        """Test avec nom trop court"""
        safe = "S1A_IW_GRD_1SDV_20230726T071112"
        result = validate_safe_name(safe)
        assert result["is_valid"] is False

    def test_valid_s2(self):
        """Test avec un SAFE Sentinel-2"""
        safe = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443"
        result = validate_safe_name(safe)

        # Sentinel-2 est supporté par ExplodeSAFE
        assert result["is_valid"] is True
        assert result["mission"] == "S2A"
        assert result["product_type"] == "MSIL1C"
        assert result["start_timestamp"] is not None

    def test_valid_s3(self):
        """Test avec un SAFE Sentinel-3"""
        safe = "S3A_SR_2_WAT____20170124T120058_20170124T121058_20170124T140548_0599_013_294______MAR_O_NR_002.SEN3"
        result = validate_safe_name(safe)

        # Sentinel-3 est supporté par ExplodeSAFE
        assert result["is_valid"] is True
        assert result["mission"] == "S3A"
        assert result["start_timestamp"] is not None


class TestValidateSafeNames:
    """Tests pour validate_safe_names()"""

    def test_mixed_valid_invalid(self, caplog):
        """Test avec un mélange de noms valides et invalides"""
        import logging

        caplog.set_level(logging.WARNING)

        safe_list = [
            "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123",
            "INVALID_NAME",
            "S1B_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123",
            "BAD_FORMAT",
        ]

        valid, invalid = validate_safe_names(safe_list, logging.getLogger())

        assert len(valid) == 2
        assert len(invalid) >= 1  # Au moins 2 invalides
        # Vérifier que les noms invalides sont bien dans la liste
        invalid_names = [i["safe_name"] for i in invalid]
        assert "INVALID_NAME" in invalid_names
        assert "BAD_FORMAT" in invalid_names


class TestDeduplicateSafeNames:
    """Tests pour deduplicate_safe_names()"""

    def test_no_duplicates(self):
        """Test avec une liste sans doublons"""
        names = ["S1A_1", "S1B_2", "S1C_3"]
        result = deduplicate_safe_names(names)
        assert result == names
        assert len(result) == 3

    def test_with_duplicates(self):
        """Test avec une liste contenant des doublons"""
        names = ["S1A_1", "S1B_2", "S1A_1", "S1C_3", "S1B_2"]
        result = deduplicate_safe_names(names)
        assert result == ["S1A_1", "S1B_2", "S1C_3"]
        assert len(result) == 3

    def test_all_duplicates(self):
        """Test avec une liste où tous les éléments sont des doublons"""
        names = ["S1A_1", "S1A_1", "S1A_1"]
        result = deduplicate_safe_names(names)
        assert result == ["S1A_1"]
        assert len(result) == 1

    def test_empty_list(self):
        """Test avec une liste vide"""
        result = deduplicate_safe_names([])
        assert result == []


class TestDetectIdentifierType:
    """Tests pour detect_identifier_type()"""

    def test_safe_name_s1(self):
        """Test avec un SAFE Sentinel-1"""
        identifier = (
            "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"
        )
        assert detect_identifier_type(identifier) == "safe_name"

    def test_safe_name_s2(self):
        """Test avec un SAFE Sentinel-2"""
        identifier = "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443"
        assert detect_identifier_type(identifier) == "safe_name"

    def test_product_id_uuid(self):
        """Test avec un Product ID (UUID)"""
        identifier = "12345678-1234-1234-1234-123456789abc"
        assert detect_identifier_type(identifier) == "product_id"

    def test_product_id_with_uppercase(self):
        """Test avec un UUID en majuscules"""
        identifier = "12345678-1234-1234-1234-123456789ABC"
        assert detect_identifier_type(identifier) == "product_id"

    def test_unknown(self):
        """Test avec un identifiant inconnu"""
        assert detect_identifier_type("random_string") == "unknown"
        assert detect_identifier_type("12345") == "unknown"
        assert detect_identifier_type("") == "unknown"
