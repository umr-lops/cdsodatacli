# retry.py
import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retry_on_exceptions: tuple = (Exception,),
):
    """Décorateur pour réessayer une fonction avec backoff exponentiel."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt+1}/{max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay = min(delay * exponential_base, max_delay)

            raise last_exception

        return wrapper

    return decorator
