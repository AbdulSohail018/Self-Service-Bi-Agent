"""Database runners for different warehouse backends."""

from app.config import Config, WarehouseType
from analytics.runners.bigquery_runner import BigQueryRunner
from analytics.runners.duckdb_runner import DuckDBRunner
from analytics.runners.snowflake_runner import SnowflakeRunner


def create_runner():
    """Factory function to create appropriate database runner based on config."""
    warehouse_type = Config.WAREHOUSE
    
    if warehouse_type == WarehouseType.DUCKDB:
        return DuckDBRunner()
    elif warehouse_type == WarehouseType.SNOWFLAKE:
        return SnowflakeRunner()
    elif warehouse_type == WarehouseType.BIGQUERY:
        return BigQueryRunner()
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse_type}")


def get_available_warehouses():
    """Get list of available warehouse types based on configuration."""
    available = [WarehouseType.DUCKDB]  # Always available
    
    # Check Snowflake configuration
    if all([
        Config.SNOWFLAKE_ACCOUNT,
        Config.SNOWFLAKE_USER,
        Config.SNOWFLAKE_PASSWORD,
        Config.SNOWFLAKE_WAREHOUSE,
        Config.SNOWFLAKE_DATABASE
    ]):
        available.append(WarehouseType.SNOWFLAKE)
    
    # Check BigQuery configuration
    if Config.BQ_PROJECT_ID and Config.BQ_DATASET:
        available.append(WarehouseType.BIGQUERY)
    
    return available


__all__ = [
    'DuckDBRunner',
    'SnowflakeRunner', 
    'BigQueryRunner',
    'create_runner',
    'get_available_warehouses'
]