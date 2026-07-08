#!/usr/bin/env python3
"""
Tests pour le rate limiter.
"""

import sys
import time
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from cdsodatacli.rate_limiter import RateLimiter


class TestRateLimiter:
    """Tests pour RateLimiter"""
    
    def test_initialization(self):
        """Test l'initialisation du rate limiter"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=20)
        assert limiter.max_requests_per_second == 10
        assert limiter.max_burst == 20
        assert limiter.tokens == 20  # Commence avec burst
        # Utiliser last_refill au lieu de last_update
        assert limiter.last_refill == pytest.approx(time.time(), abs=0.1)
    
    def test_wait_if_needed_no_wait(self):
        """Test que wait_if_needed ne bloque pas si des tokens sont disponibles"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=20)
        start = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start
        assert elapsed < 0.1  # Ne devrait pas bloquer
    
    def test_wait_if_needed_refill(self):
        """Test que les tokens sont bien rechargés"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=2)
        
        # Consommer tous les tokens
        for _ in range(2):
            limiter.wait_if_needed()
        
        # Tolérance pour les erreurs de précision flottante
        assert limiter.tokens < 0.001  # Devrait être 0 ou très proche
        
        # Attendre un peu pour que les tokens se rechargent
        time.sleep(0.2)
        # Le prochain appel devrait pouvoir passer
        start = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start
        assert elapsed < 0.1  # Ne devrait pas bloquer significativement
    
    def test_rate_limiting(self):
        """Test que le rate limiting est respecté"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=1)
        
        # Premier appel: instantané
        start = time.time()
        limiter.wait_if_needed()
        t1 = time.time() - start
        assert t1 < 0.05
        
        # Deuxième appel: doit attendre ~0.1s (1/10)
        start = time.time()
        limiter.wait_if_needed()
        t2 = time.time() - start
        assert t2 >= 0.09  # 0.1s - marge
    
    def test_burst_capacity(self):
        """Test que le burst permet des appels rapides initialement"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=5)
        
        # Les 5 premiers appels devraient être instantanés
        times = []
        for _ in range(5):
            start = time.time()
            limiter.wait_if_needed()
            times.append(time.time() - start)
        
        # Tous les appels devraient être < 0.05s
        assert all(t < 0.05 for t in times)
        
        # Le 6ème devrait attendre
        start = time.time()
        limiter.wait_if_needed()
        t6 = time.time() - start
        assert t6 >= 0.09  # ~0.1s
    
    def test_no_negative_tokens(self):
        """Test que les tokens ne deviennent pas négatifs"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=1)
        
        # Consommer le token
        limiter.wait_if_needed()
        assert limiter.tokens >= 0  # Devrait être 0 ou très proche
        
        # Forcer un refill de tokens négatif? (ne devrait pas arriver)
        # wait_if_needed devrait attendre jusqu'à ce qu'il y ait un token
        start = time.time()
        limiter.wait_if_needed()
        elapsed = time.time() - start
        assert elapsed >= 0.09  # Attend pour avoir 1 token
        assert limiter.tokens >= 0  # Consommé immédiatement après refill
    
    def test_high_throughput(self):
        """Test que le rate limiting est respecté après le burst."""
        limiter = RateLimiter(max_requests_per_second=30, max_burst=40)
        
        # 1. Consommer tout le burst
        for _ in range(limiter.max_burst):
            limiter.wait_if_needed()
        
        # 2. Mesurer le taux après le burst
        n_requests = 30
        start = time.time()
        for _ in range(n_requests):
            limiter.wait_if_needed()
        elapsed = time.time() - start
        
        rate = n_requests / elapsed
        expected_rate = limiter.max_requests_per_second
        
        print(f"\n📊 Rate after burst: {rate:.1f} req/s (expected: {expected_rate:.1f})")
        
        # Le taux doit être proche du taux nominal (tolérance 20%)
        assert rate > expected_rate * 0.8
        assert rate < expected_rate * 1.2

    def test_cdse_official_limits(self):
        """Test avec les limites officielles CDSE (2000 req/min)"""
        # 2000 req/min = 33.33 req/s
        # On utilise 30 req/s conservateur avec burst 50
        limiter = RateLimiter(max_requests_per_second=30, max_burst=50)
        
        n_requests = 150
        start = time.time()
        
        for _ in range(n_requests):
            limiter.wait_if_needed()
        
        elapsed = time.time() - start
        
        # 150 requêtes à 30 req/s = 5s
        # Avec burst de 50: (150 - 50) / 30 = 3.33s + overhead
        expected_min = (n_requests - limiter.max_burst) / limiter.max_requests_per_second
        expected_max = expected_min * 1.5
        
        assert elapsed >= expected_min - 0.5
        assert elapsed < expected_max + 1.0
        
        print(f"\n🌍 CDSE official limits test:")
        print(f"   {n_requests} requests took {elapsed:.2f}s")
        print(f"   Expected: {expected_min:.2f}s - {expected_max:.2f}s")
        print(f"   Rate: {n_requests/elapsed:.1f} req/s")
    
    def test_burst_exhaustion(self):
        """Test que le burst s'épuise et que le rate limiting prend le relais"""
        limiter = RateLimiter(max_requests_per_second=10, max_burst=3)
        
        # Consommer le burst
        for _ in range(3):
            limiter.wait_if_needed()
        
        # Maintenant les requêtes doivent être limitées à 10/s
        start = time.time()
        for _ in range(10):
            limiter.wait_if_needed()
        elapsed = time.time() - start
        
        # 10 requêtes à 10/s = ~1s (mais avec le burst initial déjà consommé)
        assert elapsed >= 0.8
        assert elapsed < 1.5
    
    def test_concurrent_requests(self):
        """Test avec des requêtes concurrentes"""
        import threading
        
        limiter = RateLimiter(max_requests_per_second=10, max_burst=5)
        results = []
        lock = threading.Lock()
        
        def make_request():
            start = time.time()
            limiter.wait_if_needed()
            with lock:
                results.append(time.time() - start)
        
        # Lancer 10 threads en parallèle
        threads = []
        for _ in range(10):
            t = threading.Thread(target=make_request)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Les 5 premiers devraient être rapides (burst), les 5 suivants plus lents
        results.sort()
        assert results[0] < 0.05  # Premier très rapide
        assert results[4] < 0.05  # 5ème aussi (burst de 5)
        assert results[5] >= 0.05  # 6ème plus lent (rate limiting)