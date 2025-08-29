"""DuckDB runner for local development and demo."""

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb
import pandas as pd

from app.config import Config


class DuckDBRunner:
    """Database runner for DuckDB (local development)."""

    def __init__(self, db_path: str = None):
        """Initialize DuckDB connection."""
        self.db_path = db_path or Config.DUCKDB_PATH
        
        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to DuckDB
        self.conn = duckdb.connect(self.db_path)
        
        # Install and load required extensions
        self._setup_extensions()

    def _setup_extensions(self):
        """Install and load DuckDB extensions."""
        try:
            # Install httpfs for remote file access (if needed)
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            
            # Install spatial extension (if needed for geo data)
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            
        except Exception as e:
            print(f"Warning: Could not install DuckDB extensions: {e}")

    def execute_query(self, sql: str, params: Dict = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Execute SQL query and return results.
        
        Returns:
            Tuple of (dataframe, metadata)
        """
        try:
            # Execute query
            if params:
                result = self.conn.execute(sql, params).fetchdf()
            else:
                result = self.conn.execute(sql).fetchdf()
            
            # Get metadata
            metadata = {
                "row_count": len(result),
                "column_count": len(result.columns),
                "columns": list(result.columns),
                "dtypes": {col: str(dtype) for col, dtype in result.dtypes.items()},
                "execution_time_ms": None,  # DuckDB doesn't provide this directly
                "warehouse": "DuckDB",
                "database_path": self.db_path
            }
            
            return result, metadata
            
        except Exception as e:
            raise Exception(f"DuckDB query execution failed: {str(e)}")

    def get_schema_info(self) -> Dict[str, List[Dict]]:
        """Get schema information for all tables."""
        schema_info = {}
        
        try:
            # Get all tables
            tables_query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
            
            tables_df = self.conn.execute(tables_query).fetchdf()
            
            for _, row in tables_df.iterrows():
                schema = row['table_schema']
                table = row['table_name']
                full_table_name = f"{schema}.{table}"
                
                # Get column information
                columns_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position
                """
                
                columns_df = self.conn.execute(columns_query).fetchdf()
                
                columns = []
                for _, col_row in columns_df.iterrows():
                    columns.append({
                        "name": col_row['column_name'],
                        "type": col_row['data_type'],
                        "nullable": col_row['is_nullable'] == 'YES',
                        "default": col_row['column_default']
                    })
                
                schema_info[full_table_name] = columns
                
        except Exception as e:
            print(f"Warning: Could not retrieve schema info: {e}")
            
        return schema_info

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            self.conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def get_table_sample(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Get sample data from a table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            raise Exception(f"Could not get sample from {table_name}: {str(e)}")

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist."""
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        except Exception as e:
            print(f"Warning: Could not create schema {schema_name}: {e}")

    def load_csv_to_table(self, csv_path: str, table_name: str, schema: str = "main") -> None:
        """Load CSV data into a table."""
        try:
            # Ensure schema exists
            if schema != "main":
                self.create_schema(schema)
            
            full_table_name = f"{schema}.{table_name}" if schema != "main" else table_name
            
            # Use DuckDB's CSV reader
            query = f"""
            CREATE OR REPLACE TABLE {full_table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}', header=true)
            """
            
            self.conn.execute(query)
            print(f"Loaded {csv_path} into {full_table_name}")
            
        except Exception as e:
            raise Exception(f"Failed to load CSV {csv_path}: {str(e)}")

    def execute_script(self, script_path: str) -> None:
        """Execute a SQL script file."""
        try:
            with open(script_path, 'r') as f:
                script_content = f.read()
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            for statement in statements:
                self.conn.execute(statement)
                
        except Exception as e:
            raise Exception(f"Failed to execute script {script_path}: {str(e)}")

    def get_query_plan(self, sql: str) -> str:
        """Get query execution plan."""
        try:
            plan_query = f"EXPLAIN {sql}"
            result = self.conn.execute(plan_query).fetchdf()
            return result.to_string()
        except Exception as e:
            return f"Could not get query plan: {str(e)}"

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats = {
            "database_path": self.db_path,
            "database_size_mb": 0,
            "connection_status": "connected" if self.test_connection() else "disconnected"
        }
        
        try:
            # Get database file size
            if os.path.exists(self.db_path):
                stats["database_size_mb"] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
            
            # Get table count
            tables_df = self.conn.execute("""
                SELECT COUNT(*) as table_count 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
            """).fetchdf()
            stats["table_count"] = int(tables_df.iloc[0]['table_count'])
            
        except Exception as e:
            print(f"Warning: Could not get database stats: {e}")
            
        return stats