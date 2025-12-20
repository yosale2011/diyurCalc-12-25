"""
Database connection and utilities for DiyurCalc application.
Provides PostgreSQL connection wrapper and database utilities.
Uses connection pooling for better performance.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool

from config import config

logger = logging.getLogger(__name__)

# Connection pool - initialized lazily
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def _get_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _connection_pool
    if _connection_pool is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL environment variable is required")
        # Create pool with 1-10 connections
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=db_url
        )
        logger.info("Database connection pool created")
    return _connection_pool


def get_pooled_connection():
    """Get a connection from the pool."""
    return _get_pool().getconn()


def return_connection(conn):
    """Return a connection to the pool."""
    if _connection_pool is not None:
        _connection_pool.putconn(conn)


class PostgresConnection:
    """Wrapper for PostgreSQL connection to provide SQLite-like interface.
    Uses connection pooling for better performance."""

    def __init__(self, conn, use_pool: bool = True):
        self.conn = conn
        self._in_transaction = False
        self._use_pool = use_pool

    def execute(self, query: str, params: tuple = ()) -> Any:
        """Execute a query and return a cursor-like object."""
        # Convert SQLite placeholders (?) to PostgreSQL (%s)
        query = query.replace("?", "%s")
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        return cursor

    def cursor(self, *args, **kwargs):
        """Allow raw access to cursors if needed (e.g. by logic.py functions)."""
        return self.conn.cursor(*args, **kwargs)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if self._use_pool:
            return_connection(self.conn)
        else:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_conn() -> PostgresConnection:
    """Create and return a PostgreSQL database connection wrapped with SQLite-like interface.
    Uses connection pooling for better performance."""
    pg_conn = get_pooled_connection()
    return PostgresConnection(pg_conn, use_pool=True)
