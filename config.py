"""Load configuration from .env file at project root."""

import os
from pathlib import Path


def load_env(env_path: str | Path | None = None) -> dict[str, str]:
    """Read key=value pairs from .env file into os.environ and return them."""
    if env_path is None:
        env_path = Path(__file__).resolve().parent / ".env"
    env_path = Path(env_path)
    values = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if key:
                os.environ.setdefault(key, val)
                values[key] = val
    return values


# Auto-load on import
_env = load_env()

LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_USER = os.environ.get("LAKEBASE_USER", "")
LAKEBASE_PASSWORD = os.environ.get("LAKEBASE_PASSWORD", "")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "")
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LAKEBASE_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "email_to_quote")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "")
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "EMAIL_QUOTE")
CATALOG = "dvin100_email_to_quote"
SCHEMA = "email_to_quote"
