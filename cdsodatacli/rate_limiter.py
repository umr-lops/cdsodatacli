# rate_limiter.py
import time
from threading import Lock


class RateLimiter:
    """Rate limiter thread-safe basé sur un bucket de jetons."""

    def __init__(self, max_requests_per_second: float = 10.0, max_burst: int = 20):
        self.max_requests_per_second = max_requests_per_second
        self.max_burst = max_burst
        self.tokens = max_burst
        self.last_refill = time.time()
        self.lock = Lock()

    def wait_if_needed(self):
        """Attend si le nombre de requêtes autorisées est atteint."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            new_tokens = elapsed * self.max_requests_per_second
            self.tokens = min(self.max_burst, self.tokens + new_tokens)
            self.last_refill = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.max_requests_per_second
                time.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1
