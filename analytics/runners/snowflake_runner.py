"""Snowflake runner for cloud data warehouse."""

from typing import Any, Dict, List, Tuple

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from app.config import Config


class SnowflakeRunner:
    """Database runner for Snowflake cloud data warehouse."""

    def __init__(self):
        """Initialize Snowflake connection."""
        self.config = Config
        self.conn = None
        self.cursor = None
        self._connect()

    def _connect(self):
        """Establish connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(
                account=self.config.SNOWFLAKE_ACCOUNT,
                user=self.config.SNOWFLAKE_USER,
                password=self.config.SNOWFLAKE_PASSWORD,
                warehouse=self.config.SNOWFLAKE_WAREHOUSE,
                database=self.config.SNOWFLAKE_DATABASE,
                schema=self.config.SNOWFLAKE_SCHEMA,
                role=self.config.SNOWFLAKE_ROLE,
                session_parameters={
                    'QUERY_TAG': 'BI_ASSISTANT',
                    'TIMEZONE': self.config.DEFAULT_TIMEZONE
                }
            )
            self.cursor = self.conn.cursor()
            print(f"Connected to Snowflake: {self.config.SNOWFLAKE_ACCOUNT}")
            
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {str(e)}")

    def execute_query(self, sql: str, params: Dict = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Execute SQL query and return results.
        
        Returns:
            Tuple of (dataframe, metadata)
        """
        try:
            # Execute query with timeout
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            # Fetch results
            result_df = self.cursor.fetch_pandas_all()
            
            # Get query metadata
            query_id = self.cursor.sfqid
            
            # Get execution statistics
            stats_query = f"""
            SELECT 
                QUERY_ID,
                EXECUTION_TIME,
                COMPILATION_TIME,
                ROWS_PRODUCED,
                BYTES_SCANNED,
                WAREHOUSE_SIZE
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID = '{query_id}'
            """
            
            try:
                self.cursor.execute(stats_query)
                stats_df = self.cursor.fetch_pandas_all()
                execution_time = int(stats_df.iloc[0]['EXECUTION_TIME']) if not stats_df.empty else None
                rows_produced = int(stats_df.iloc[0]['ROWS_PRODUCED']) if not stats_df.empty else None
                bytes_scanned = int(stats_df.iloc[0]['BYTES_SCANNED']) if not stats_df.empty else None
            except:
                execution_time = None
                rows_produced = None
                bytes_scanned = None
            
            metadata = {
                "row_count": len(result_df),
                "column_count": len(result_df.columns),
                "columns": list(result_df.columns),
                "dtypes": {col: str(dtype) for col, dtype in result_df.dtypes.items()},
                "execution_time_ms": execution_time,
                "rows_produced": rows_produced,
                "bytes_scanned": bytes_scanned,
                "query_id": query_id,
                "warehouse": "Snowflake",
                "warehouse_size": self.config.SNOWFLAKE_WAREHOUSE
            }
            
            return result_df, metadata
            
        except Exception as e:
            raise Exception(f"Snowflake query execution failed: {str(e)}")

    def get_schema_info(self) -> Dict[str, List[Dict]]:
        """Get schema information for all accessible tables."""
        schema_info = {}
        
        try:
            # Get all tables in the current database and schema
            tables_query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            
            self.cursor.execute(tables_query)
            tables_df = self.cursor.fetch_pandas_all()
            
            for _, row in tables_df.iterrows():
                schema = row['TABLE_SCHEMA']
                table = row['TABLE_NAME']
                full_table_name = f"{schema}.{table}"
                
                # Get column information
                columns_query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION
                """
                
                self.cursor.execute(columns_query)
                columns_df = self.cursor.fetch_pandas_all()
                
                columns = []
                for _, col_row in columns_df.iterrows():
                    columns.append({
                        "name": col_row['COLUMN_NAME'],
                        "type": col_row['DATA_TYPE'],
                        "nullable": col_row['IS_NULLABLE'] == 'YES',
                        "default": col_row['COLUMN_DEFAULT'],
                        "comment": col_row['COMMENT']
                    })
                
                schema_info[full_table_name] = columns
                
        except Exception as e:
            print(f"Warning: Could not retrieve schema info: {e}")
            
        return schema_info

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            self.cursor.execute("SELECT CURRENT_VERSION()")
            return True
        except Exception:
            return False

    def get_table_sample(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Get sample data from a table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            self.cursor.execute(query)
            return self.cursor.fetch_pandas_all()
        except Exception as e:
            raise Exception(f"Could not get sample from {table_name}: {str(e)}")

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist."""
        try:
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        except Exception as e:
            print(f"Warning: Could not create schema {schema_name}: {e}")

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, schema: str = None) -> None:
        """Load pandas DataFrame into a Snowflake table."""
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Use Snowflake pandas connector
            success, nchunks, nrows, _ = write_pandas(
                self.conn,
                df,
                table_name=table_name.upper(),
                schema=schema.upper() if schema else None,
                auto_create_table=True,
                overwrite=True
            )
            
            if success:
                print(f"Loaded {nrows} rows into {full_table_name}")
            else:
                raise Exception("Failed to load data")
                
        except Exception as e:
            raise Exception(f"Failed to load DataFrame: {str(e)}")

    def execute_script(self, script_content: str) -> None:
        """Execute a SQL script."""
        try:
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            for statement in statements:
                self.cursor.execute(statement)
                
        except Exception as e:
            raise Exception(f"Failed to execute script: {str(e)}")

    def get_query_plan(self, sql: str) -> str:
        """Get query execution plan."""
        try:
            plan_query = f"EXPLAIN {sql}"
            self.cursor.execute(plan_query)
            result_df = self.cursor.fetch_pandas_all()
            return result_df.to_string()
        except Exception as e:
            return f"Could not get query plan: {str(e)}"

    def get_warehouse_usage(self) -> Dict[str, Any]:
        """Get warehouse usage statistics."""
        try:
            usage_query = """
            SELECT 
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) as TOTAL_CREDITS,
                SUM(EXECUTION_TIME) as TOTAL_EXECUTION_TIME_MS
            FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
                DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
                DATE_RANGE_END => CURRENT_DATE()
            ))
            WHERE WAREHOUSE_NAME = CURRENT_WAREHOUSE()
            GROUP BY WAREHOUSE_NAME
            """
            
            self.cursor.execute(usage_query)
            result_df = self.cursor.fetch_pandas_all()
            
            if not result_df.empty:
                return {
                    "warehouse_name": result_df.iloc[0]['WAREHOUSE_NAME'],
                    "credits_used_7d": float(result_df.iloc[0]['TOTAL_CREDITS']),
                    "execution_time_ms_7d": int(result_df.iloc[0]['TOTAL_EXECUTION_TIME_MS'])
                }
            else:
                return {"warehouse_name": self.config.SNOWFLAKE_WAREHOUSE, "credits_used_7d": 0}
                
        except Exception as e:
            print(f"Warning: Could not get warehouse usage: {e}")
            return {"warehouse_name": self.config.SNOWFLAKE_WAREHOUSE, "error": str(e)}

    def close(self) -> None:
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get database connection statistics."""
        stats = {
            "warehouse": self.config.SNOWFLAKE_WAREHOUSE,
            "database": self.config.SNOWFLAKE_DATABASE,
            "schema": self.config.SNOWFLAKE_SCHEMA,
            "account": self.config.SNOWFLAKE_ACCOUNT,
            "connection_status": "connected" if self.test_connection() else "disconnected"
        }
        
        try:
            # Get current session info
            self.cursor.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            session_info = self.cursor.fetchone()
            
            stats.update({
                "snowflake_version": session_info[0],
                "current_warehouse": session_info[1],
                "current_database": session_info[2],
                "current_schema": session_info[3]
            })
            
            # Add warehouse usage
            stats.update(self.get_warehouse_usage())
            
        except Exception as e:
            print(f"Warning: Could not get session stats: {e}")
            
        return stats