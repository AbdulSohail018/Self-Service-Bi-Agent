"""Configuration management for the BI Assistant application."""

import os
from enum import Enum
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class WarehouseType(str, Enum):
    """Supported warehouse types."""
    DUCKDB = "DUCKDB"
    SNOWFLAKE = "SNOWFLAKE"
    BIGQUERY = "BIGQUERY"


class LLMProvider(str, Enum):
    """Supported LLM providers."""
    OLLAMA = "ollama"
    OPENAI = "openai"


class VectorBackend(str, Enum):
    """Supported vector database backends."""
    CHROMA = "chroma"
    FAISS = "faiss"


class Config:
    """Application configuration."""

    # Warehouse settings
    WAREHOUSE: WarehouseType = WarehouseType(os.getenv("WAREHOUSE", "DUCKDB"))
    DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", ".data/local.duckdb")

    # Snowflake settings
    SNOWFLAKE_ACCOUNT: Optional[str] = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_USER: Optional[str] = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD: Optional[str] = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_WAREHOUSE: Optional[str] = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE: Optional[str] = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA: Optional[str] = os.getenv("SNOWFLAKE_SCHEMA")
    SNOWFLAKE_ROLE: Optional[str] = os.getenv("SNOWFLAKE_ROLE")

    # BigQuery settings
    BQ_PROJECT_ID: Optional[str] = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET: Optional[str] = os.getenv("BQ_DATASET")
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # LLM settings
    LLM_PROVIDER: LLMProvider = LLMProvider(os.getenv("LLM_PROVIDER", "ollama"))
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "llama3.1")
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")

    # Vector database settings
    VECTOR_BACKEND: VectorBackend = VectorBackend(os.getenv("VECTOR_BACKEND", "chroma"))
    VECTOR_DIR: str = os.getenv("VECTOR_DIR", ".data/vector")

    # Application settings
    APP_PORT: int = int(os.getenv("APP_PORT", "8501"))
    DEFAULT_TIMEZONE: str = os.getenv("DEFAULT_TIMEZONE", "America/Los_Angeles")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Query limits and safety
    MAX_ROWS: int = int(os.getenv("MAX_ROWS", "10000"))
    QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "300"))  # 5 minutes
    
    # Schema allowlist patterns
    ALLOWED_SCHEMAS: list[str] = [
        "marts.people.*",
        "staging.*",
        "seeds.*"
    ]

    @classmethod
    def validate(cls) -> None:
        """Validate configuration based on selected warehouse."""
        if cls.WAREHOUSE == WarehouseType.SNOWFLAKE:
            required_snowflake_vars = [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER", 
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE"
            ]
            for var in required_snowflake_vars:
                if not getattr(cls, var):
                    raise ValueError(f"{var} is required for Snowflake connection")
        
        elif cls.WAREHOUSE == WarehouseType.BIGQUERY:
            if not cls.BQ_PROJECT_ID or not cls.BQ_DATASET:
                raise ValueError("BQ_PROJECT_ID and BQ_DATASET are required for BigQuery")
        
        if cls.LLM_PROVIDER == LLMProvider.OPENAI and not cls.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY is required for OpenAI provider")

    @classmethod
    def get_warehouse_display_name(cls) -> str:
        """Get display name for current warehouse."""
        return cls.WAREHOUSE.value.title()

    @classmethod
    def is_local_mode(cls) -> bool:
        """Check if running in local mode (DuckDB)."""
        return cls.WAREHOUSE == WarehouseType.DUCKDB