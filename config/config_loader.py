"""
Configuration management for Markets Data Hub.
"""

import os
from pathlib import Path
from typing import Any, Optional
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Centralized configuration management."""

    def __init__(self, env: str = None):
        """
        Initialize configuration.

        Args:
            env: Environment (dev/staging/prod), defaults to ENV variable or 'dev'
        """
        self.env = env or os.getenv("ENV", "dev")
        self.project_root = Path(__file__).parent.parent.parent
        self._config = self._load_config()

    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        config_file = self.project_root / "config" / "config.yaml"

        if config_file.exists():
            with open(config_file, "r") as f:
                return yaml.safe_load(f) or {}
        return {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.

        Args:
            key: Configuration key (dot notation supported)
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    # Databricks settings
    @property
    def databricks_host(self) -> str:
        return os.getenv("DATABRICKS_HOST", self.get("databricks.host", ""))

    @property
    def databricks_token(self) -> str:
        return os.getenv("DATABRICKS_TOKEN", "")

    @property
    def databricks_cluster_id(self) -> str:
        return os.getenv("DATABRICKS_CLUSTER_ID", self.get("databricks.cluster_id", ""))

    # Data paths
    @property
    def bronze_path(self) -> str:
        return os.getenv(
            "BRONZE_PATH", self.get("data.bronze_path", "dbfs:/mnt/bronze")
        )

    @property
    def silver_path(self) -> str:
        return os.getenv(
            "SILVER_PATH", self.get("data.silver_path", "dbfs:/mnt/silver")
        )

    @property
    def gold_path(self) -> str:
        return os.getenv("GOLD_PATH", self.get("data.gold_path", "dbfs:/mnt/gold"))


# Singleton instance
_config_instance: Optional[Config] = None


def get_config() -> Config:
    """Get singleton config instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance
