"""Tests for DuckDB runner functionality."""

import pytest
import tempfile
import os
from pathlib import Path

import pandas as pd

from analytics.runners.duckdb_runner import DuckDBRunner


class TestDuckDBRunner:
    """Test cases for DuckDB database runner."""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path for testing."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def runner(self, temp_db_path):
        """Create DuckDB runner instance for testing."""
        return DuckDBRunner(temp_db_path)

    def test_connection_initialization(self, runner):
        """Test that runner initializes and connects successfully."""
        assert runner.conn is not None
        assert runner.test_connection() is True

    def test_basic_query_execution(self, runner):
        """Test basic SQL query execution."""
        sql = "SELECT 1 as test_value, 'hello' as test_string"
        
        df, metadata = runner.execute_query(sql)
        
        assert len(df) == 1
        assert df.iloc[0]['test_value'] == 1
        assert df.iloc[0]['test_string'] == 'hello'
        assert metadata['row_count'] == 1
        assert metadata['column_count'] == 2
        assert metadata['warehouse'] == 'DuckDB'

    def test_create_schema(self, runner):
        """Test schema creation."""
        runner.create_schema('test_schema')
        
        # Verify schema was created
        result = runner.conn.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'test_schema'").fetchall()
        assert len(result) > 0

    def test_table_creation_and_data_insertion(self, runner):
        """Test table creation and data insertion."""
        # Create test table
        create_sql = """
        CREATE TABLE test_employees (
            id INTEGER,
            name VARCHAR,
            department VARCHAR,
            salary INTEGER
        )
        """
        runner.conn.execute(create_sql)
        
        # Insert test data
        insert_sql = """
        INSERT INTO test_employees VALUES 
        (1, 'John Doe', 'Engineering', 80000),
        (2, 'Jane Smith', 'Marketing', 70000),
        (3, 'Bob Johnson', 'Sales', 60000)
        """
        runner.conn.execute(insert_sql)
        
        # Query data
        df, metadata = runner.execute_query("SELECT * FROM test_employees ORDER BY id")
        
        assert len(df) == 3
        assert df.iloc[0]['name'] == 'John Doe'
        assert df.iloc[1]['department'] == 'Marketing'
        assert metadata['row_count'] == 3

    def test_csv_loading(self, runner):
        """Test loading CSV data into table."""
        # Create test CSV data
        test_data = pd.DataFrame({
            'employee_id': ['EMP001', 'EMP002', 'EMP003'],
            'name': ['Alice', 'Bob', 'Charlie'],
            'department': ['HR', 'IT', 'Finance'],
            'salary': [50000, 70000, 60000]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            test_data.to_csv(f.name, index=False)
            csv_path = f.name
        
        try:
            # Load CSV to table
            runner.load_csv_to_table(csv_path, 'employees', 'main')
            
            # Verify data was loaded
            df, metadata = runner.execute_query("SELECT * FROM employees ORDER BY employee_id")
            
            assert len(df) == 3
            assert df.iloc[0]['employee_id'] == 'EMP001'
            assert df.iloc[1]['name'] == 'Bob'
            assert metadata['row_count'] == 3
            
        finally:
            # Cleanup
            os.unlink(csv_path)

    def test_schema_info_retrieval(self, runner):
        """Test schema information retrieval."""
        # Create test table with known structure
        create_sql = """
        CREATE TABLE test_schema_info (
            id INTEGER NOT NULL,
            name VARCHAR(100),
            created_date DATE,
            is_active BOOLEAN DEFAULT true
        )
        """
        runner.conn.execute(create_sql)
        
        # Get schema info
        schema_info = runner.get_schema_info()
        
        assert 'main.test_schema_info' in schema_info
        columns = schema_info['main.test_schema_info']
        
        column_names = [col['name'] for col in columns]
        assert 'id' in column_names
        assert 'name' in column_names
        assert 'created_date' in column_names
        assert 'is_active' in column_names

    def test_table_sample_retrieval(self, runner):
        """Test retrieving sample data from table."""
        # Create and populate test table
        runner.conn.execute("CREATE TABLE sample_test (id INTEGER, value VARCHAR)")
        runner.conn.execute("INSERT INTO sample_test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
        
        # Get sample
        sample_df = runner.get_table_sample('sample_test', limit=3)
        
        assert len(sample_df) == 3
        assert 'id' in sample_df.columns
        assert 'value' in sample_df.columns

    def test_script_execution(self, runner):
        """Test SQL script execution."""
        script_content = """
        CREATE TABLE script_test (id INTEGER, name VARCHAR);
        INSERT INTO script_test VALUES (1, 'test1');
        INSERT INTO script_test VALUES (2, 'test2');
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(script_content)
            script_path = f.name
        
        try:
            runner.execute_script(script_path)
            
            # Verify script execution
            df, _ = runner.execute_query("SELECT * FROM script_test ORDER BY id")
            assert len(df) == 2
            assert df.iloc[0]['name'] == 'test1'
            
        finally:
            os.unlink(script_path)

    def test_query_plan_retrieval(self, runner):
        """Test query execution plan retrieval."""
        # Create test table
        runner.conn.execute("CREATE TABLE plan_test (id INTEGER, name VARCHAR)")
        
        plan = runner.get_query_plan("SELECT * FROM plan_test WHERE id > 100")
        
        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_database_stats(self, runner):
        """Test database statistics retrieval."""
        stats = runner.get_stats()
        
        assert 'database_path' in stats
        assert 'connection_status' in stats
        assert stats['connection_status'] == 'connected'
        assert 'database_size_mb' in stats

    def test_context_manager_usage(self, temp_db_path):
        """Test using runner as context manager."""
        with DuckDBRunner(temp_db_path) as runner:
            assert runner.test_connection() is True
            df, _ = runner.execute_query("SELECT 1 as test")
            assert len(df) == 1
        
        # Connection should be closed after context exit
        # Note: DuckDB doesn't prevent further use after close, but we test the pattern

    def test_error_handling_invalid_sql(self, runner):
        """Test error handling for invalid SQL."""
        with pytest.raises(Exception):
            runner.execute_query("INVALID SQL QUERY HERE")

    def test_error_handling_nonexistent_table(self, runner):
        """Test error handling for queries on nonexistent tables."""
        with pytest.raises(Exception):
            runner.execute_query("SELECT * FROM nonexistent_table")

    def test_multiple_schemas(self, runner):
        """Test working with multiple schemas."""
        # Create schemas
        runner.create_schema('schema1')
        runner.create_schema('schema2')
        
        # Create tables in different schemas
        runner.conn.execute("CREATE TABLE schema1.table1 (id INTEGER)")
        runner.conn.execute("CREATE TABLE schema2.table2 (id INTEGER)")
        
        # Insert data
        runner.conn.execute("INSERT INTO schema1.table1 VALUES (1), (2)")
        runner.conn.execute("INSERT INTO schema2.table2 VALUES (3), (4)")
        
        # Query from specific schemas
        df1, _ = runner.execute_query("SELECT * FROM schema1.table1")
        df2, _ = runner.execute_query("SELECT * FROM schema2.table2")
        
        assert len(df1) == 2
        assert len(df2) == 2
        assert df1.iloc[0]['id'] == 1
        assert df2.iloc[0]['id'] == 3

    def test_large_dataset_handling(self, runner):
        """Test handling of larger datasets."""
        # Create table with more data
        runner.conn.execute("CREATE TABLE large_test (id INTEGER, value DOUBLE)")
        
        # Insert 1000 rows
        insert_sql = "INSERT INTO large_test SELECT i, random() FROM range(1000) as t(i)"
        runner.conn.execute(insert_sql)
        
        # Query data
        df, metadata = runner.execute_query("SELECT * FROM large_test")
        
        assert len(df) == 1000
        assert metadata['row_count'] == 1000
        assert 'id' in df.columns
        assert 'value' in df.columns

    def test_data_types_handling(self, runner):
        """Test handling of various data types."""
        create_sql = """
        CREATE TABLE types_test (
            int_col INTEGER,
            float_col DOUBLE,
            string_col VARCHAR,
            date_col DATE,
            bool_col BOOLEAN,
            timestamp_col TIMESTAMP
        )
        """
        runner.conn.execute(create_sql)
        
        insert_sql = """
        INSERT INTO types_test VALUES (
            42,
            3.14159,
            'test string',
            '2024-01-15',
            true,
            '2024-01-15 10:30:00'
        )
        """
        runner.conn.execute(insert_sql)
        
        df, _ = runner.execute_query("SELECT * FROM types_test")
        
        assert len(df) == 1
        assert df.iloc[0]['int_col'] == 42
        assert abs(df.iloc[0]['float_col'] - 3.14159) < 0.0001
        assert df.iloc[0]['string_col'] == 'test string'
        assert df.iloc[0]['bool_col'] is True