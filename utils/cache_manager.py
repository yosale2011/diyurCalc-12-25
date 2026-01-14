"""
Cache management module for DiyurCalc application.
Provides in-memory caching with TTL support for expensive calculations and database queries.
"""

from __future__ import annotations
import time
import hashlib
import json
import logging
from functools import wraps
from typing import Any, Optional, Callable, Dict, Tuple
from datetime import datetime, timedelta
import threading

logger = logging.getLogger(__name__)


class CacheManager:
    """
    Thread-safe in-memory cache with TTL (Time To Live) support.
    """

    def __init__(self, default_ttl: int = 300):
        """
        Initialize cache manager.

        Args:
            default_ttl: Default TTL in seconds (5 minutes)
        """
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.default_ttl = default_ttl
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def _make_key(self, prefix: str, *args, **kwargs) -> str:
        """
        Create a cache key from function arguments.

        Args:
            prefix: Prefix for the cache key
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Hashed cache key
        """
        key_data = {
            'prefix': prefix,
            'args': args,
            'kwargs': sorted(kwargs.items())
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if it exists and hasn't expired.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        with self.lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if time.time() < expiry:
                    self.hits += 1
                    logger.debug(f"Cache hit: {key}")
                    return value
                else:
                    # Remove expired entry
                    del self.cache[key]
                    logger.debug(f"Cache expired: {key}")

            self.misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Set value in cache with TTL.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (uses default if None)
        """
        ttl = ttl or self.default_ttl
        expiry = time.time() + ttl

        with self.lock:
            self.cache[key] = (value, expiry)
            logger.debug(f"Cache set: {key}, TTL: {ttl}s")

    def delete(self, key: str):
        """Delete a specific key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                logger.debug(f"Cache deleted: {key}")

    def clear(self, prefix: Optional[str] = None):
        """
        Clear cache entries.

        Args:
            prefix: If provided, only clear keys starting with this prefix
        """
        with self.lock:
            if prefix:
                keys_to_delete = [k for k in self.cache.keys() if k.startswith(prefix)]
                for key in keys_to_delete:
                    del self.cache[key]
                logger.info(f"Cleared {len(keys_to_delete)} cache entries with prefix: {prefix}")
            else:
                self.cache.clear()
                logger.info("Cache cleared completely")

    def cleanup_expired(self):
        """Remove all expired entries from cache."""
        current_time = time.time()
        with self.lock:
            expired_keys = [
                key for key, (_, expiry) in self.cache.items()
                if current_time >= expiry
            ]
            for key in expired_keys:
                del self.cache[key]

            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0

            return {
                'entries': len(self.cache),
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': f"{hit_rate:.2f}%",
                'memory_usage': self._estimate_memory_usage()
            }

    def _estimate_memory_usage(self) -> str:
        """Estimate memory usage of cache in human-readable format."""
        import sys
        total_size = 0
        for key, (value, _) in self.cache.items():
            total_size += sys.getsizeof(key)
            total_size += sys.getsizeof(value)

        if total_size < 1024:
            return f"{total_size} bytes"
        elif total_size < 1024 * 1024:
            return f"{total_size / 1024:.2f} KB"
        else:
            return f"{total_size / (1024 * 1024):.2f} MB"


# Global cache instance
cache = CacheManager()


def cached(ttl: int = 300, key_prefix: Optional[str] = None):
    """
    Decorator to cache function results.

    Args:
        ttl: Time to live in seconds
        key_prefix: Optional prefix for cache key (defaults to function name)

    Usage:
        @cached(ttl=600)
        def expensive_calculation(param1, param2):
            # ... expensive operation
            return result

    Note: Cache keys automatically include demo_mode to prevent cross-contamination
    between production and demo database results.
    """
    def decorator(func: Callable) -> Callable:
        prefix = key_prefix or f"{func.__module__}.{func.__name__}"

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Import here to avoid circular imports
            from core.database import is_demo_mode

            # Include demo_mode in cache key to separate prod/demo results
            demo_suffix = "_demo" if is_demo_mode() else "_prod"
            cache_key = cache._make_key(prefix + demo_suffix, *args, **kwargs)

            # Check cache
            result = cache.get(cache_key)
            if result is not None:
                return result

            # Calculate result
            result = func(*args, **kwargs)

            # Store in cache
            cache.set(cache_key, result, ttl)

            return result

        # Add cache control methods to the wrapper
        wrapper.cache_clear = lambda: cache.clear(prefix)
        wrapper.cache_stats = lambda: cache.get_stats()

        return wrapper
    return decorator


def cache_key_builder(prefix: str, **params) -> str:
    """
    Build a cache key from parameters.

    Args:
        prefix: Key prefix
        **params: Parameters to include in key

    Returns:
        Cache key string
    """
    return cache._make_key(prefix, **params)


# Specific cache decorators for common use cases

def cache_employee_data(ttl: int = 600):
    """Cache employee data for 10 minutes by default."""
    return cached(ttl=ttl, key_prefix="employee")


def cache_report_data(ttl: int = 300):
    """Cache report data for 5 minutes by default."""
    return cached(ttl=ttl, key_prefix="report")


def cache_shabbat_times(ttl: int = 86400):
    """Cache Shabbat times for 24 hours by default."""
    return cached(ttl=ttl, key_prefix="shabbat")


def cache_calculation_result(ttl: int = 1800):
    """Cache calculation results for 30 minutes by default."""
    return cached(ttl=ttl, key_prefix="calculation")


# Background cleanup task
def start_cache_cleanup_task(interval: int = 300):
    """
    Start a background thread to clean up expired cache entries.

    Args:
        interval: Cleanup interval in seconds (default: 5 minutes)
    """
    def cleanup_loop():
        while True:
            time.sleep(interval)
            try:
                cache.cleanup_expired()
                stats = cache.get_stats()
                logger.info(f"Cache cleanup completed. Stats: {stats}")
            except Exception as e:
                logger.error(f"Cache cleanup failed: {e}")

    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
    cleanup_thread.start()
    logger.info(f"Cache cleanup task started (interval: {interval}s)")


# Request-scoped cache for web applications
class RequestCache:
    """
    Request-scoped cache that lives only for the duration of a single HTTP request.
    Useful for avoiding repeated database queries within the same request.
    """

    def __init__(self):
        self.data: Dict[str, Any] = {}

    def get(self, key: str, generator: Optional[Callable] = None) -> Any:
        """
        Get value from request cache or generate it.

        Args:
            key: Cache key
            generator: Function to generate value if not in cache

        Returns:
            Cached or generated value
        """
        if key not in self.data and generator:
            self.data[key] = generator()
        return self.data.get(key)

    def set(self, key: str, value: Any):
        """Set value in request cache."""
        self.data[key] = value

    def clear(self):
        """Clear request cache."""
        self.data.clear()