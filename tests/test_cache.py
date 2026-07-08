#!/usr/bin/env python3
"""
Tests pour le cache.
"""

import sys
import os
import json
import tempfile
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from cdsodatacli.get_products_metadata import get_cache_filename


class TestCache:
    """Tests pour les fonctions de cache"""
    
    def test_get_cache_filename(self):
        """Test la génération de nom de fichier de cache"""
        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Name eq 'S1A_...'"
        filename = get_cache_filename(url)
        
        assert filename.endswith('.json')
        # MD5 = 32 caractères + '.json' = 5 caractères = 37 caractères au total
        assert len(filename) == 37  # 32 chars MD5 + 5 chars '.json'
        
        # La même URL devrait donner le même nom
        filename2 = get_cache_filename(url)
        assert filename == filename2
    
    def test_get_cache_filename_with_dir(self):
        """Test avec un répertoire de cache"""
        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Name eq 'S1A_...'"
        cache_dir = "/tmp/cdsodatacli_cache"
        filename = get_cache_filename(url, cache_dir)
        
        assert filename.startswith(cache_dir)
        assert filename.endswith('.json')
        # Le nom complet inclut le chemin
        assert len(filename) > 37
    
    def test_cache_consistency(self):
        """Test que l'URL est correctement hashée"""
        url1 = "https://example.com?filter=name1"
        url2 = "https://example.com?filter=name2"
        
        filename1 = get_cache_filename(url1)
        filename2 = get_cache_filename(url2)
        
        # Les URLs différentes devraient donner des noms différents
        assert filename1 != filename2
        
        # La même URL devrait donner le même nom
        filename1_bis = get_cache_filename(url1)
        assert filename1 == filename1_bis
    
    def test_cache_filename_format(self):
        """Test le format du nom de fichier de cache"""
        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Name eq 'S1A_...'"
        filename = get_cache_filename(url)
        
        # Vérifier que c'est bien un hash hexadécimal + .json
        basename = os.path.basename(filename)
        name_parts = basename.split('.')
        
        assert len(name_parts) == 2
        assert name_parts[1] == 'json'
        # Le hash doit être hexadécimal (0-9a-f)
        assert all(c in '0123456789abcdef' for c in name_parts[0])
        assert len(name_parts[0]) == 32