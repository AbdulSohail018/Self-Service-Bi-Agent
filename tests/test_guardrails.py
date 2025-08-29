"""Tests for SQL guardrails and safety validation."""

import pytest

from analytics.nl2sql.guardrails import SQLGuardrails, validate_query


class TestSQLGuardrails:
    """Test cases for SQL safety guardrails."""

    @pytest.fixture
    def guardrails(self):
        """Create guardrails instance for testing."""
        return SQLGuardrails(
            max_rows=1000,
            allowed_schemas=['marts.people.*', 'staging.*', 'seeds.*']
        )

    def test_valid_select_query(self, guardrails):
        """Test that valid SELECT queries pass validation."""
        sql = "SELECT employee_id, department FROM marts.people.dim_employees WHERE is_active = true"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert error == ""
        assert "LIMIT" in cleaned_sql

    def test_blocked_delete_statement(self, guardrails):
        """Test that DELETE statements are blocked."""
        sql = "DELETE FROM marts.people.dim_employees WHERE department = 'Sales'"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked keyword" in error.lower()

    def test_blocked_update_statement(self, guardrails):
        """Test that UPDATE statements are blocked."""
        sql = "UPDATE marts.people.dim_employees SET salary = 100000 WHERE employee_id = 'EMP001'"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked keyword" in error.lower()

    def test_blocked_drop_statement(self, guardrails):
        """Test that DROP statements are blocked."""
        sql = "DROP TABLE marts.people.dim_employees"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked keyword" in error.lower()

    def test_blocked_create_statement(self, guardrails):
        """Test that CREATE statements are blocked."""
        sql = "CREATE TABLE test_table (id INTEGER, name VARCHAR)"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked keyword" in error.lower()

    def test_row_limit_enforcement(self, guardrails):
        """Test that row limits are enforced."""
        sql = "SELECT * FROM marts.people.dim_employees"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "LIMIT 1000" in cleaned_sql

    def test_existing_limit_respected(self, guardrails):
        """Test that existing LIMIT clauses are respected if within bounds."""
        sql = "SELECT * FROM marts.people.dim_employees LIMIT 500"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "LIMIT 500" in cleaned_sql

    def test_excessive_limit_reduced(self, guardrails):
        """Test that excessive LIMIT values are reduced."""
        sql = "SELECT * FROM marts.people.dim_employees LIMIT 50000"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "LIMIT 1000" in cleaned_sql

    def test_schema_allowlist_enforcement(self, guardrails):
        """Test that only allowed schemas can be accessed."""
        # Allowed schema
        sql1 = "SELECT * FROM marts.people.dim_employees"
        is_valid1, error1, _ = guardrails.validate_sql(sql1)
        assert is_valid1 is True
        
        # Disallowed schema
        sql2 = "SELECT * FROM hr_system.sensitive_data"
        is_valid2, error2, _ = guardrails.validate_sql(sql2)
        assert is_valid2 is False
        assert "not allowed" in error2.lower()

    def test_sql_comment_removal(self, guardrails):
        """Test that SQL comments are properly removed."""
        sql = """
        SELECT employee_id, department -- This is a comment
        FROM marts.people.dim_employees
        /* Multi-line
           comment here */
        WHERE is_active = true
        """
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "--" not in cleaned_sql
        assert "/*" not in cleaned_sql

    def test_empty_query_rejection(self, guardrails):
        """Test that empty queries are rejected."""
        sql = ""
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "empty" in error.lower()

    def test_whitespace_only_query_rejection(self, guardrails):
        """Test that whitespace-only queries are rejected."""
        sql = "   \n\t   "
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "empty" in error.lower()

    def test_dangerous_function_blocking(self, guardrails):
        """Test that dangerous functions are blocked."""
        sql = "SELECT LOAD_FILE('/etc/passwd') as data"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked function" in error.lower()

    def test_union_query_validation(self, guardrails):
        """Test that UNION queries are properly validated."""
        sql = """
        SELECT employee_id, department FROM marts.people.dim_employees
        UNION
        SELECT employee_id, job_title FROM marts.people.dim_employees
        """
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "UNION" in cleaned_sql

    def test_cte_query_validation(self, guardrails):
        """Test that CTE (Common Table Expression) queries are validated."""
        sql = """
        WITH department_stats AS (
            SELECT department, COUNT(*) as emp_count
            FROM marts.people.dim_employees
            GROUP BY department
        )
        SELECT * FROM department_stats
        """
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert "WITH" in cleaned_sql

    def test_subquery_validation(self, guardrails):
        """Test that subqueries are properly validated."""
        sql = """
        SELECT employee_id, department
        FROM marts.people.dim_employees
        WHERE department IN (
            SELECT department 
            FROM marts.people.dim_employees 
            GROUP BY department 
            HAVING COUNT(*) > 10
        )
        """
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True

    def test_case_insensitive_keyword_blocking(self, guardrails):
        """Test that keyword blocking is case-insensitive."""
        sql = "delete from marts.people.dim_employees"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False
        assert "blocked keyword" in error.lower()

    def test_multiple_statements_blocked(self, guardrails):
        """Test that multiple statements in one query are handled."""
        sql = "SELECT * FROM marts.people.dim_employees; DROP TABLE dim_employees;"
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is False

    def test_convenience_function(self):
        """Test the convenience validation function."""
        sql = "SELECT * FROM marts.people.dim_employees"
        
        is_valid, error, cleaned_sql = validate_query(sql)
        
        assert is_valid is True
        assert error == ""
        assert "LIMIT" in cleaned_sql

    def test_sql_injection_prevention(self, guardrails):
        """Test prevention of common SQL injection patterns."""
        malicious_sqls = [
            "SELECT * FROM users WHERE id = 1; DROP TABLE users; --",
            "SELECT * FROM users WHERE name = 'admin' OR '1'='1'",
            "SELECT * FROM users UNION SELECT password FROM admin_users",
        ]
        
        for sql in malicious_sqls:
            is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
            # These should either be blocked or heavily sanitized
            assert is_valid is False or "DROP" not in cleaned_sql.upper()

    def test_complex_valid_analytics_query(self, guardrails):
        """Test a complex but valid analytics query."""
        sql = """
        SELECT 
            e.department,
            DATE_TRUNC('month', ae.termination_date) as month,
            COUNT(ae.event_id) as terminations,
            COUNT(DISTINCT e.employee_id) as total_employees,
            ROUND(COUNT(ae.event_id) * 100.0 / COUNT(DISTINCT e.employee_id), 2) as attrition_rate
        FROM marts.people.dim_employees e
        LEFT JOIN marts.people.fct_attrition_events ae ON e.employee_id = ae.employee_id
        WHERE ae.termination_date >= '2024-01-01'
        GROUP BY e.department, DATE_TRUNC('month', ae.termination_date)
        ORDER BY month DESC, attrition_rate DESC
        """
        
        is_valid, error, cleaned_sql = guardrails.validate_sql(sql)
        
        assert is_valid is True
        assert error == ""
        assert all(keyword in cleaned_sql.upper() for keyword in ['SELECT', 'FROM', 'LEFT JOIN', 'GROUP BY', 'ORDER BY'])