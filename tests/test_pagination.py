# tests/test_pagination.py
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import cdsodatacli.query as qr
from collections import defaultdict
from cdsodatacli.query import DEFAULT_TOP_ROWS_PER_QUERY
import requests

class TestPagination:
    """Tests pour la pagination des requêtes OData."""
    
    def test_pagination_single_page(self):
        """Test qu'une requête avec < 1000 produits ne fait qu'une page."""
        mock_data = [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(500)]
        mock_response = MagicMock()
        mock_response.json.return_value = {'value': mock_data}
        
        with patch('requests.get', return_value=mock_response):
            cpt = defaultdict(int)
            cpt, df = qr.fetch_one_url_with_pagination(
                'http://test.url?$top=1000',
                cpt,
                'test_index',
                top=1000
            )
            assert len(df) == 500
            assert cpt['urls_tested'] == 1
    
    def test_pagination_multiple_pages(self):
        """Test qu'une requête avec > 1000 produits fait plusieurs pages."""
        # Simuler 3 pages avec un total de 2500 produits
        def mock_get(url, *args, **kwargs):
            mock = MagicMock()
            if 'skip=2000' in url:
                # Dernière page: 500 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(2000, 2500)]}
            elif 'skip=1000' in url:
                # Deuxième page: 1000 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(1000, 2000)]}
            else:
                # Première page: 1000 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(1000)]}
            return mock
        
        with patch('requests.get', side_effect=mock_get):
            cpt = defaultdict(int)
            cpt, df = qr.fetch_one_url_with_pagination(
                'http://test.url?$top=1000',
                cpt,
                'test_index',
                top=1000
            )
            assert len(df) == 2500
            assert cpt['urls_tested'] == 3
            assert cpt['pages_fetched'] == 3
    
    def test_pagination_cache(self):
        """Test que le cache fonctionne avec la pagination."""
        cached_data = [{'Id': 'cached_1', 'Name': 'cached_name_1'}]
        
        with patch('os.path.exists', return_value=True):
            with patch('builtins.open', MagicMock()):
                with patch('json.load', return_value={'value': cached_data}):
                    cpt = defaultdict(int)
                    cpt, df = qr.fetch_one_url_with_pagination(
                        'http://test.url?$top=1000',
                        cpt,
                        'test_index',
                        cache_dir='/tmp/cache'
                    )
                    assert cpt['cache_used'] == 1
                    assert len(df) == 1
                    assert df.iloc[0]['Id'] == 'cached_1'
    
    def test_pagination_custom_top(self):
        """Test avec un nombre de produits par page personnalisé."""
        # Simuler 3 pages avec top=100
        total_products = 250
        
        def mock_get(url, *args, **kwargs):
            mock = MagicMock()
            # Extraire le skip de l'URL
            if 'skip=200' in url:
                # Dernière page: 50 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(200, 250)]}
            elif 'skip=100' in url:
                # Deuxième page: 100 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(100, 200)]}
            else:
                # Première page: 100 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(100)]}
            return mock
        
        with patch('requests.get', side_effect=mock_get):
            cpt = defaultdict(int)
            cpt, df = qr.fetch_one_url_with_pagination(
                'http://test.url?$top=100',
                cpt,
                'test_index',
                top=100
            )
            assert len(df) == 250
            assert cpt['urls_tested'] == 3
    
    def test_pagination_no_top_in_url(self):
        """Test quand l'URL n'a pas de paramètre $top."""
        total_products = 150
        
        def mock_get(url, *args, **kwargs):
            mock = MagicMock()
            if 'skip=100' in url:
                # Deuxième page: 50 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(100, 150)]}
            else:
                # Première page: 100 produits
                mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(100)]}
            return mock
        
        with patch('requests.get', side_effect=mock_get):
            cpt = defaultdict(int)
            cpt, df = qr.fetch_one_url_with_pagination(
                'http://test.url?filter=name',
                cpt,
                'test_index',
                top=100
            )
            assert len(df) == 150
            assert cpt['urls_tested'] == 2
    
    def test_pagination_http_error(self):
        """Test la gestion des erreurs HTTP pendant la pagination."""
        call_count = 0
        
        def mock_get(url, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock = MagicMock()
            # La première page est OK, la deuxième échoue
            if 'skip=1000' in url:
                mock.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Internal Server Error")
                return mock
            # Première page OK
            mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(1000)]}
            return mock
        
        with patch('requests.get', side_effect=mock_get):
            with patch('cdsodatacli.query.logger') as mock_logger:
                cpt = defaultdict(int)
                # On désactive le retry pour le test en patchant la fonction
                with patch('cdsodatacli.query._fetch_with_retry', side_effect=requests.exceptions.HTTPError("500 Internal Server Error")):
                    # Le test doit lever l'exception après les retries
                    with pytest.raises(requests.exceptions.HTTPError):
                        cpt, df = qr.fetch_one_url_with_pagination(
                            'http://test.url?$top=1000',
                            cpt,
                            'test_index',
                            top=1000
                        )
    
    def test_pagination_max_skip_limit(self):
        """Test que la pagination s'arrête à max_skip."""
        # Simuler des pages infinies de 1000 produits
        def mock_get(url, *args, **kwargs):
            mock = MagicMock()
            mock.json.return_value = {'value': [{'Id': f'id_{i}', 'Name': f'name_{i}'} for i in range(1000)]}
            return mock
        
        with patch('requests.get', side_effect=mock_get):
            cpt = defaultdict(int)
            cpt, df = qr.fetch_one_url_with_pagination(
                'http://test.url?$top=1000',
                cpt,
                'test_index',
                top=1000,
                max_skip=5000  # Limite à 5000
            )
            # Devrait avoir 5 pages de 1000 = 5000 produits
            assert len(df) == 5000
            assert cpt['pages_fetched'] == 5
            assert cpt['urls_tested'] == 5