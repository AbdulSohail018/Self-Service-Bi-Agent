"""BigQuery runner for Google Cloud data warehouse."""

import os
from typing import Any, Dict, List, Tuple

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from app.config import Config


class BigQueryRunner:
    """Database runner for Google BigQuery."""

    def __init__(self):
        """Initialize BigQuery connection."""
        self.config = Config
        self.client = None
        self._connect()

    def _connect(self):
        """Establish connection to BigQuery."""
        try:
            # Set up credentials
            if self.config.GOOGLE_APPLICATION_CREDENTIALS:
                credentials = service_account.Credentials.from_service_account_file(
                    self.config.GOOGLE_APPLICATION_CREDENTIALS
                )
                self.client = bigquery.Client(
                    project=self.config.BQ_PROJECT_ID,
                    credentials=credentials
                )
            else:
                # Use default credentials (e.g., from environment)
                self.client = bigquery.Client(project=self.config.BQ_PROJECT_ID)
            
            # Test connection
            query_job = self.client.query("SELECT 1 as test")
            query_job.result()
            
            print(f"Connected to BigQuery project: {self.config.BQ_PROJECT_ID}")
            
        except Exception as e:
            raise Exception(f"Failed to connect to BigQuery: {str(e)}")

    def execute_query(self, sql: str, params: Dict = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Execute SQL query and return results.
        
        Returns:
            Tuple of (dataframe, metadata)
        """
        try:
            # Configure job
            job_config = bigquery.QueryJobConfig()
            
            # Set query parameters if provided
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(key, "STRING", value)
                    for key, value in params.items()
                ]
            
            # Set labels for tracking
            job_config.labels = {"source": "bi_assistant"}
            
            # Add timeout
            job_config.job_timeout_ms = self.config.QUERY_TIMEOUT * 1000
            
            # Execute query
            query_job = self.client.query(sql, job_config=job_config)
            
            # Get results as DataFrame
            result_df = query_job.to_dataframe()
            
            # Collect metadata
            metadata = {
                "row_count": len(result_df),
                "column_count": len(result_df.columns),
                "columns": list(result_df.columns),
                "dtypes": {col: str(dtype) for col, dtype in result_df.dtypes.items()},
                "job_id": query_job.job_id,
                "bytes_processed": query_job.total_bytes_processed,
                "bytes_billed": query_job.total_bytes_billed,
                "slot_ms": query_job.slot_millis,
                "creation_time": query_job.created.isoformat() if query_job.created else None,
                "execution_time_ms": None,  # Calculate if needed
                "warehouse": "BigQuery",
                "project": self.config.BQ_PROJECT_ID,
                "dataset": self.config.BQ_DATASET
            }
            
            # Calculate execution time
            if query_job.created and query_job.ended:
                execution_time = (query_job.ended - query_job.created).total_seconds() * 1000
                metadata["execution_time_ms"] = int(execution_time)
            
            return result_df, metadata
            
        except Exception as e:
            raise Exception(f"BigQuery query execution failed: {str(e)}")

    def get_schema_info(self) -> Dict[str, List[Dict]]:
        """Get schema information for all accessible tables."""
        schema_info = {}
        
        try:
            # List all datasets accessible to the user
            datasets = list(self.client.list_datasets())
            
            for dataset in datasets:
                dataset_id = dataset.dataset_id
                
                # Get tables in this dataset
                dataset_ref = self.client.dataset(dataset_id)
                tables = list(self.client.list_tables(dataset_ref))
                
                for table in tables:
                    table_ref = dataset_ref.table(table.table_id)
                    table_obj = self.client.get_table(table_ref)
                    
                    full_table_name = f"{dataset_id}.{table.table_id}"
                    
                    # Get column information
                    columns = []
                    for field in table_obj.schema:
                        columns.append({
                            "name": field.name,
                            "type": field.field_type,
                            "mode": field.mode,
                            "nullable": field.mode == "NULLABLE",
                            "description": field.description
                        })
                    
                    schema_info[full_table_name] = columns
                    
        except Exception as e:
            print(f"Warning: Could not retrieve schema info: {e}")
            
        return schema_info

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            query_job = self.client.query("SELECT 1")
            query_job.result()
            return True
        except Exception:
            return False

    def get_table_sample(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Get sample data from a table."""
        try:
            # Ensure table name is properly formatted
            if '.' not in table_name:
                table_name = f"{self.config.BQ_DATASET}.{table_name}"
            
            query = f"SELECT * FROM `{self.config.BQ_PROJECT_ID}.{table_name}` LIMIT {limit}"
            query_job = self.client.query(query)
            return query_job.to_dataframe()
        except Exception as e:
            raise Exception(f"Could not get sample from {table_name}: {str(e)}")

    def create_dataset(self, dataset_id: str, location: str = "US") -> None:
        """Create a dataset if it doesn't exist."""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset.description = "Created by BI Assistant"
            
            self.client.create_dataset(dataset, exists_ok=True)
            print(f"Created dataset: {dataset_id}")
            
        except Exception as e:
            print(f"Warning: Could not create dataset {dataset_id}: {e}")

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, dataset_id: str = None) -> None:
        """Load pandas DataFrame into a BigQuery table."""
        try:
            dataset_id = dataset_id or self.config.BQ_DATASET
            table_ref = self.client.dataset(dataset_id).table(table_name)
            
            # Configure job
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.autodetect = True
            
            # Load data
            job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete
            
            print(f"Loaded {len(df)} rows into {dataset_id}.{table_name}")
            
        except Exception as e:
            raise Exception(f"Failed to load DataFrame: {str(e)}")

    def execute_script(self, script_content: str) -> None:
        """Execute a SQL script (multiple statements)."""
        try:
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script_content.split(';') if stmt.strip()]
            
            for statement in statements:
                query_job = self.client.query(statement)
                query_job.result()  # Wait for completion
                
        except Exception as e:
            raise Exception(f"Failed to execute script: {str(e)}")

    def get_query_plan(self, sql: str) -> str:
        """Get query execution plan (dry run)."""
        try:
            job_config = bigquery.QueryJobConfig()
            job_config.dry_run = True
            
            query_job = self.client.query(sql, job_config=job_config)
            
            plan_info = {
                "total_bytes_processed": query_job.total_bytes_processed,
                "total_bytes_billed": query_job.total_bytes_billed,
                "estimated_cost_usd": (query_job.total_bytes_billed / (1024**4)) * 5.0  # Rough estimate
            }
            
            return str(plan_info)
            
        except Exception as e:
            return f"Could not get query plan: {str(e)}"

    def get_job_history(self, limit: int = 10) -> List[Dict]:
        """Get recent job history."""
        try:
            jobs = []
            for job in self.client.list_jobs(max_results=limit):
                if job.job_type == "query":
                    jobs.append({
                        "job_id": job.job_id,
                        "created": job.created.isoformat() if job.created else None,
                        "ended": job.ended.isoformat() if job.ended else None,
                        "state": job.state,
                        "bytes_processed": job.total_bytes_processed,
                        "bytes_billed": job.total_bytes_billed,
                        "slot_ms": job.slot_millis
                    })
            
            return jobs
            
        except Exception as e:
            print(f"Warning: Could not get job history: {e}")
            return []

    def close(self) -> None:
        """Close database connection (BigQuery client doesn't need explicit closing)."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get BigQuery connection and usage statistics."""
        stats = {
            "project": self.config.BQ_PROJECT_ID,
            "dataset": self.config.BQ_DATASET,
            "connection_status": "connected" if self.test_connection() else "disconnected"
        }
        
        try:
            # Get dataset info
            dataset_ref = self.client.dataset(self.config.BQ_DATASET)
            dataset = self.client.get_dataset(dataset_ref)
            
            stats.update({
                "dataset_location": dataset.location,
                "dataset_created": dataset.created.isoformat() if dataset.created else None,
                "dataset_modified": dataset.modified.isoformat() if dataset.modified else None
            })
            
            # Get table count in dataset
            tables = list(self.client.list_tables(dataset_ref))
            stats["table_count"] = len(tables)
            
            # Get recent job info
            recent_jobs = self.get_job_history(limit=5)
            if recent_jobs:
                total_bytes_processed = sum(job.get("bytes_processed", 0) or 0 for job in recent_jobs)
                stats["recent_bytes_processed"] = total_bytes_processed
                stats["recent_job_count"] = len(recent_jobs)
            
        except Exception as e:
            print(f"Warning: Could not get BigQuery stats: {e}")
            
        return stats