"""
Configuration management for DiyurCalc application.
Centralizes all configuration settings and environment variables.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Central configuration class for the application."""

    # Application version
    VERSION: str = "2.06"

    # Database configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")

    # Application configuration
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")

    # Server configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))

    # Application paths
    BASE_DIR: Path = Path(__file__).parent
    TEMPLATES_DIR: Path = BASE_DIR / "templates"
    STATIC_DIR: Optional[Path] = BASE_DIR / "static" if (BASE_DIR / "static").exists() else None

    # Feature flags
    ENABLE_CACHING: bool = os.getenv("ENABLE_CACHING", "True").lower() in ("true", "1", "yes")
    CACHE_TIMEOUT: int = int(os.getenv("CACHE_TIMEOUT", "300"))  # 5 minutes

    # Export configuration
    DEFAULT_EXPORT_ENCODING: str = os.getenv("DEFAULT_EXPORT_ENCODING", "utf-8")

    # Wage configuration
    DEFAULT_MINIMUM_WAGE: float = float(os.getenv("DEFAULT_MINIMUM_WAGE", "34.40"))

    # Calculation constants
    STANDARD_WORK_DAYS_PER_MONTH: float = 21.66
    MAX_SICK_DAYS_PER_MONTH: float = 1.5
    LOCAL_TZ = ZoneInfo("Asia/Jerusalem")

    def __init__(self):
        """Validate configuration on initialization."""
        if not self.DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL environment variable is required. "
                "Please set it in .env file."
            )

        if self.SECRET_KEY == "your-secret-key-change-in-production":
            print("WARNING: Using default SECRET_KEY. Change this in production!")

    @classmethod
    def from_env(cls) -> Config:
        """Create Config instance from environment variables."""
        return cls()

    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.DEBUG

    def is_production(self) -> bool:
        """Check if running in production mode."""
        return not self.DEBUG


# Global config instance
config = Config.from_env()
