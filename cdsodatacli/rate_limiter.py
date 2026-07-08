#!/usr/bin/env python3
"""
Rate Limiter for CDSE OData API.
Thread-safe token bucket implementation.
"""

import time
from threading import Lock
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter thread-safe basé sur un bucket de jetons.
    """

    def __init__(self, max_requests_per_second: float = 30.0, max_burst: int = 50):
        self.max_requests_per_second = max_requests_per_second
        self.max_burst = max_burst
        self.tokens = float(max_burst)
        self.last_refill = time.time()
        self.lock = Lock()

        logger.debug(
            f"RateLimiter initialized: rate={max_requests_per_second} req/s, "
            f"burst={max_burst}"
        )

    def _refill(self) -> None:
        """Recharge les jetons en fonction du temps écoulé."""
        now = time.time()
        elapsed = now - self.last_refill
        if elapsed > 0:
            self.tokens = min(
                self.max_burst, self.tokens + elapsed * self.max_requests_per_second
            )
            self.last_refill = now

    def wait_if_needed(self) -> None:
        """
        Attend si nécessaire pour respecter la limite de taux.
        """
        with self.lock:
            self._refill()

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            # Pas de jeton disponible : calculer le temps d'attente
            wait_time = (1.0 - self.tokens) / self.max_requests_per_second

            # IMPORTANT: On attend avec le lock pour éviter les interférences
            # mais on recharge APRÈS l'attente
            time.sleep(wait_time)

            # Après l'attente, recharger et consommer un jeton
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                # Fallback (devrait rarement arriver)
                # On force la consommation d'un jeton après un petit sleep
                time.sleep(0.001)
                self.tokens = 0.0

    def get_available_tokens(self) -> float:
        """Retourne le nombre de jetons disponibles."""
        with self.lock:
            self._refill()
            return self.tokens

    def reset(self) -> None:
        """Réinitialise le rate limiter à pleine capacité."""
        with self.lock:
            self.tokens = float(self.max_burst)
            self.last_refill = time.time()
            logger.debug("RateLimiter reset to full capacity")


# ── INSTANCES GLOBALES ────────────────────────────────────────────────────

# Instance standard pour une utilisation normale (conservatrice)
_GLOBAL_RATE_LIMITER = RateLimiter(max_requests_per_second=30.0, max_burst=50)

# Instance agressive pour les tests ou lorsque le système peut supporter plus
_AGGRESSIVE_RATE_LIMITER = RateLimiter(max_requests_per_second=35.0, max_burst=60)
