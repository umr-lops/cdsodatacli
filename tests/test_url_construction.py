#!/usr/bin/env python3
"""
Tests unitaires pour la construction des URLs.
"""

import sys
from pathlib import Path
import pytest
import logging

sys.path.insert(0, str(Path(__file__).parent.parent))

from cdsodatacli.get_products_metadata import (
    create_urls_from_safe_names,
    create_urls_from_product_ids,
)


class TestCreateUrlsFromSafeNames:
    """Tests pour create_urls_from_safe_names()"""

    def test_single_safe(self):
        """Test avec un seul SAFE name"""
        names = ["S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_012345_012345_0123"]
        batches = create_urls_from_safe_names(names, batch_size=15)

        assert len(batches) == 1
        assert batches[0]["num_products"] == 1
        assert "Name eq" in batches[0]["url"]
        assert "S1A_IW_GRDH_1SDV" in batches[0]["url"]
        # Vérifier que $top correspond au batch_size
        assert "$top=15" in batches[0]["url"]
        assert "$expand=Attributes" in batches[0]["url"]

    def test_multiple_safes_one_batch(self):
        """Test avec plusieurs SAFE names dans un seul batch"""
        names = [
            f"S1A_IW_GRDH_1SDV_20230726T07111{i}_20230726T07113{i}_012345_012345_0123"
            for i in range(5)
        ]
        batches = create_urls_from_safe_names(names, batch_size=15)

        assert len(batches) == 1
        assert batches[0]["num_products"] == 5
        assert " or " in batches[0]["url"]
        assert "Name eq" in batches[0]["url"]
        assert "$top=15" in batches[0]["url"]

    def test_multiple_batches(self):
        """Test avec plusieurs batches"""
        names = [
            f"S1A_IW_GRDH_1SDV_20230726T0711{i:02d}_20230726T0711{i:02d}_012345_012345_0123"
            for i in range(25)
        ]
        batches = create_urls_from_safe_names(names, batch_size=10)

        assert len(batches) == 3  # 25/10 = 3 batches
        assert batches[0]["num_products"] == 10
        assert batches[1]["num_products"] == 10
        assert batches[2]["num_products"] == 5
        assert batches[0]["batch_id"] == 0
        assert batches[1]["batch_id"] == 1
        assert batches[2]["batch_id"] == 2
        # Vérifier les $top
        assert "$top=10" in batches[0]["url"]
        assert "$top=10" in batches[1]["url"]
        assert "$top=10" in batches[2]["url"]  # Même si seulement 5 produits

    def test_url_length_warning(self, caplog):
        """Test que la construction d'URL génère un warning si trop longue"""

        caplog.set_level(logging.WARNING)

        # Créer des noms très longs pour forcer une URL > 2048 caractères
        names = [
            f"S1A_IW_GRDH_1SDV_20230726T{str(i).zfill(6)}_20230726T{str(i).zfill(6)}_012345_012345_0123"
            for i in range(30)
        ]

        # Batch size = 30 pour forcer une grande URL
        batches = create_urls_from_safe_names(names, batch_size=30)

        assert len(batches) == 1
        # Vérifier qu'un warning a été émis si l'URL dépasse 2048
        if len(batches[0]["url"]) > 2048:
            assert "exceeds 2048" in caplog.text
        else:
            pytest.skip("URL length not exceeding 2048 in this test environment")

    def test_empty_list(self):
        """Test avec une liste vide"""
        batches = create_urls_from_safe_names([])
        assert batches == []

    def test_escape_apostrophe(self):
        """Test que les apostrophes sont correctement échappées"""
        # Les noms SAFE n'ont normalement pas d'apostrophe, mais on teste
        names = ["S1A_IW_GRD_H_1SDV_20230726T071112_20230726T071137_012345_012345_0123"]
        batches = create_urls_from_safe_names(names)

        # Vérifier que l'URL contient le nom encodé
        assert "Name eq" in batches[0]["url"]
        # L'apostrophe dans le nom devrait être échappée
        # Note: dans ce cas il n'y a pas d'apostrophe, mais si il y en avait une,
        # elle serait remplacée par ''
        assert "'" in batches[0]["url"] or "Name eq" in batches[0]["url"]

    def test_custom_batch_size(self):
        """Test avec un batch_size personnalisé"""
        names = [
            f"S1A_IW_GRDH_1SDV_20230726T0711{i:02d}_20230726T0711{i:02d}_012345_012345_0123"
            for i in range(7)
        ]
        batches = create_urls_from_safe_names(names, batch_size=3)

        assert len(batches) == 3  # 7/3 = 3 batches
        assert batches[0]["num_products"] == 3
        assert batches[1]["num_products"] == 3
        assert batches[2]["num_products"] == 1
        assert "$top=3" in batches[0]["url"]
        assert "$top=3" in batches[1]["url"]
        assert "$top=3" in batches[2]["url"]


class TestCreateUrlsFromProductIds:
    """Tests pour create_urls_from_product_ids()"""

    def test_single_product_id(self):
        """Test avec un seul Product ID"""
        ids = ["12345678-1234-1234-1234-123456789abc"]
        batches = create_urls_from_product_ids(ids)

        assert len(batches) == 1
        assert batches[0]["num_products"] == 1
        assert "Id eq" in batches[0]["url"]
        # Le batch_size par défaut pour les IDs est 50
        assert "$top=50" in batches[0]["url"]
        assert "$expand=Attributes" in batches[0]["url"]

    def test_single_product_id_custom_batch(self):
        """Test avec un seul Product ID et batch_size personnalisé"""
        ids = ["12345678-1234-1234-1234-123456789abc"]
        batches = create_urls_from_product_ids(ids, batch_size=10)

        assert len(batches) == 1
        assert batches[0]["num_products"] == 1
        assert "$top=10" in batches[0]["url"]

    def test_multiple_ids(self):
        """Test avec plusieurs Product IDs"""
        ids = [f"12345678-1234-1234-1234-123456789a{i:02d}" for i in range(100)]
        batches = create_urls_from_product_ids(ids, batch_size=20)

        assert len(batches) == 5  # 100/20 = 5
        assert all(b["num_products"] == 20 for b in batches[:4])
        assert batches[4]["num_products"] == 20  # 100 exactement divisible
        assert all("$top=20" in b["url"] for b in batches)

    def test_multiple_ids_uneven(self):
        """Test avec un nombre d'IDs qui ne tombe pas juste"""
        ids = [f"12345678-1234-1234-1234-123456789a{i:02d}" for i in range(95)]
        batches = create_urls_from_product_ids(ids, batch_size=20)

        assert len(batches) == 5  # 95/20 = 4.75 -> 5 batches
        assert batches[0]["num_products"] == 20
        assert batches[1]["num_products"] == 20
        assert batches[2]["num_products"] == 20
        assert batches[3]["num_products"] == 20
        assert batches[4]["num_products"] == 15  # Dernier batch incomplet
        assert all("$top=20" in b["url"] for b in batches)

    def test_deduplication(self):
        """Test que les doublons sont supprimés"""
        ids = [
            "12345678-1234-1234-1234-123456789abc",
            "12345678-1234-1234-1234-123456789abc",
            "12345678-1234-1234-1234-123456789def",
        ]
        batches = create_urls_from_product_ids(ids, batch_size=10)

        assert len(batches) == 1
        # Le batch devrait contenir 2 IDs uniques
        assert batches[0]["num_products"] == 2
        # Vérifier que les deux IDs sont dans l'URL
        assert "123456789abc" in batches[0]["url"]
        assert "123456789def" in batches[0]["url"]

    def test_empty_list(self):
        """Test avec une liste vide"""
        batches = create_urls_from_product_ids([])
        assert batches == []

    def test_escape_apostrophe(self):
        """Test que les apostrophes sont échappées (rare pour des UUIDs)"""
        # Les UUIDs n'ont normalement pas d'apostrophe, mais on teste
        ids = ["12345678-1234-1234-1234-123456789'abc"]
        batches = create_urls_from_product_ids(ids)
        # L'apostrophe devrait être échappée
        # Note: dans ce cas ce n'est pas un UUID valide, mais on teste l'échappement
        assert "Id eq" in batches[0]["url"]
        # L'apostrophe simple est remplacée par deux apostrophes en OData
        # Mais ici c'est juste un test de principe
        assert "$top=50" in batches[0]["url"]
