"""SQL safety and validation guardrails."""

import re
from typing import List, Tuple

import sqlglot
from sqlglot import parse_one, transpile
from sqlglot.expressions import Expression

from app.config import Config


class SQLGuardrails:
    """SQL safety validator to prevent dangerous operations."""

    # Dangerous keywords that should be blocked
    BLOCKED_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE',
        'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL', 'MERGE', 'REPLACE'
    }

    # Functions that should be blocked for security
    BLOCKED_FUNCTIONS = {
        'LOAD_FILE', 'INTO OUTFILE', 'INTO DUMPFILE', 'LOAD DATA',
        'SYSTEM', 'SHELL', 'EVAL', 'EXEC'
    }

    def __init__(self, max_rows: int = None, allowed_schemas: List[str] = None):
        """Initialize guardrails with configuration."""
        self.max_rows = max_rows or Config.MAX_ROWS
        self.allowed_schemas = allowed_schemas or Config.ALLOWED_SCHEMAS

    def validate_sql(self, sql: str) -> Tuple[bool, str, str]:
        """
        Validate SQL query for safety and compliance.
        
        Returns:
            Tuple of (is_valid, error_message, cleaned_sql)
        """
        try:
            # Basic syntax validation
            sql = sql.strip()
            if not sql:
                return False, "Empty SQL query", ""

            # Remove comments and normalize
            cleaned_sql = self._clean_sql(sql)

            # Check for blocked keywords
            is_valid, error = self._check_blocked_keywords(cleaned_sql)
            if not is_valid:
                return False, error, ""

            # Parse SQL with sqlglot
            try:
                parsed = parse_one(cleaned_sql, dialect="duckdb")
            except Exception as e:
                return False, f"SQL parsing error: {str(e)}", ""

            # Validate SELECT-only operations
            if not self._is_select_only(parsed):
                return False, "Only SELECT statements are allowed", ""

            # Check schema allowlist
            is_valid, error = self._check_schema_allowlist(parsed)
            if not is_valid:
                return False, error, ""

            # Enforce row limit
            cleaned_sql = self._enforce_row_limit(cleaned_sql)

            # Validate functions
            is_valid, error = self._check_blocked_functions(cleaned_sql)
            if not is_valid:
                return False, error, ""

            return True, "", cleaned_sql

        except Exception as e:
            return False, f"Validation error: {str(e)}", ""

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL query."""
        # Remove SQL comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Normalize whitespace
        sql = ' '.join(sql.split())
        
        return sql.strip()

    def _check_blocked_keywords(self, sql: str) -> Tuple[bool, str]:
        """Check for dangerous SQL keywords."""
        sql_upper = sql.upper()
        
        for keyword in self.BLOCKED_KEYWORDS:
            # Use word boundaries to avoid false positives
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, sql_upper):
                return False, f"Blocked keyword detected: {keyword}"
        
        return True, ""

    def _check_blocked_functions(self, sql: str) -> Tuple[bool, str]:
        """Check for dangerous SQL functions."""
        sql_upper = sql.upper()
        
        for func in self.BLOCKED_FUNCTIONS:
            if func in sql_upper:
                return False, f"Blocked function detected: {func}"
        
        return True, ""

    def _is_select_only(self, parsed: Expression) -> bool:
        """Verify query contains only SELECT statements."""
        # sqlglot expression type checking
        from sqlglot.expressions import Select, Union, CTE
        
        if isinstance(parsed, (Select, Union, CTE)):
            return True
        
        return False

    def _check_schema_allowlist(self, parsed: Expression) -> Tuple[bool, str]:
        """Validate that all tables/schemas are in allowlist."""
        # Extract table references from parsed SQL
        tables = self._extract_table_references(parsed)
        
        for table in tables:
            if not self._is_table_allowed(table):
                return False, f"Access to table '{table}' is not allowed"
        
        return True, ""

    def _extract_table_references(self, parsed: Expression) -> List[str]:
        """Extract all table references from parsed SQL."""
        tables = []
        
        # Use sqlglot to find all table references
        for table in parsed.find_all(sqlglot.expressions.Table):
            if table.name:
                schema_table = ""
                if table.db:
                    schema_table += table.db + "."
                if table.catalog:
                    schema_table = table.catalog + "." + schema_table
                schema_table += table.name
                tables.append(schema_table)
        
        return tables

    def _is_table_allowed(self, table: str) -> bool:
        """Check if table matches allowed schema patterns."""
        for pattern in self.allowed_schemas:
            # Convert glob pattern to regex
            regex_pattern = pattern.replace('*', '.*').replace('?', '.')
            if re.match(f"^{regex_pattern}$", table, re.IGNORECASE):
                return True
        
        return False

    def _enforce_row_limit(self, sql: str) -> str:
        """Add or modify LIMIT clause to enforce row limits."""
        sql_upper = sql.upper()
        
        # Check if LIMIT already exists
        limit_match = re.search(r'\bLIMIT\s+(\d+)\b', sql_upper)
        
        if limit_match:
            # Extract existing limit
            existing_limit = int(limit_match.group(1))
            if existing_limit > self.max_rows:
                # Replace with max allowed
                sql = re.sub(
                    r'\bLIMIT\s+\d+\b', 
                    f'LIMIT {self.max_rows}', 
                    sql, 
                    flags=re.IGNORECASE
                )
        else:
            # Add LIMIT clause
            sql = sql.rstrip(';') + f' LIMIT {self.max_rows}'
        
        return sql

    def get_safe_sql_template(self, user_sql: str) -> str:
        """Generate a safe SQL template with parameterized queries."""
        # This would be used for preparing parameterized queries
        # For now, return the validated SQL
        is_valid, error, cleaned_sql = self.validate_sql(user_sql)
        
        if not is_valid:
            raise ValueError(f"SQL validation failed: {error}")
        
        return cleaned_sql


def validate_query(sql: str) -> Tuple[bool, str, str]:
    """Convenience function for SQL validation."""
    guardrails = SQLGuardrails()
    return guardrails.validate_sql(sql)