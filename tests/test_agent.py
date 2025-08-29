"""Tests for the NL→SQL agent."""

import pytest
from unittest.mock import Mock, patch

from analytics.nl2sql.agent import NL2SQLAgent, create_agent
from analytics.runners.duckdb_runner import DuckDBRunner


class TestNL2SQLAgent:
    """Test cases for NL2SQL agent functionality."""

    @pytest.fixture
    def mock_runner(self):
        """Create mock warehouse runner."""
        runner = Mock(spec=DuckDBRunner)
        runner.test_connection.return_value = True
        runner.get_schema_info.return_value = {
            'marts.people.dim_employees': [
                {'name': 'employee_id', 'type': 'VARCHAR'},
                {'name': 'department', 'type': 'VARCHAR'},
                {'name': 'salary', 'type': 'INTEGER'}
            ]
        }
        return runner

    @pytest.fixture
    def agent(self, mock_runner):
        """Create agent instance for testing."""
        with patch('analytics.nl2sql.agent.Config') as mock_config:
            mock_config.LLM_PROVIDER.value = "ollama"
            mock_config.OLLAMA_MODEL = "llama3.1"
            
            with patch('analytics.nl2sql.agent.ChatOllama') as mock_llm:
                mock_response = Mock()
                mock_response.content = "SELECT department, COUNT(*) as headcount FROM marts.people.dim_employees WHERE is_active = true GROUP BY department LIMIT 1000"
                mock_llm.return_value.return_value = [mock_response]
                
                agent = NL2SQLAgent(mock_runner)
                return agent

    def test_agent_initialization(self, mock_runner):
        """Test agent initializes correctly."""
        with patch('analytics.nl2sql.agent.Config') as mock_config:
            mock_config.LLM_PROVIDER.value = "ollama"
            mock_config.OLLAMA_MODEL = "llama3.1"
            
            with patch('analytics.nl2sql.agent.ChatOllama'):
                agent = NL2SQLAgent(mock_runner)
                assert agent.warehouse_runner == mock_runner
                assert agent.guardrails is not None
                assert agent.schema_index is not None

    def test_simple_headcount_query(self, agent):
        """Test simple headcount query translation."""
        query = "What's the current headcount by department?"
        
        with patch.object(agent.schema_index, 'get_relevant_context') as mock_context:
            mock_context.return_value = {
                "schema": [{"metadata": {"name": "dim_employees", "columns": ["department", "employee_id"]}}],
                "metrics": []
            }
            
            with patch.object(agent.llm, '__call__') as mock_llm:
                mock_response = Mock()
                mock_response.content = "SELECT department, COUNT(*) as headcount FROM marts.people.dim_employees WHERE is_active = true GROUP BY department LIMIT 1000"
                mock_llm.return_value = mock_response
                
                success, sql, error = agent.translate_to_sql(query)
                
                assert success is True
                assert "SELECT" in sql
                assert "department" in sql.lower()
                assert "count" in sql.lower()
                assert "limit" in sql.lower()
                assert error == ""

    def test_sql_guardrails_validation(self, agent):
        """Test that SQL guardrails prevent dangerous queries."""
        with patch.object(agent.llm, '__call__') as mock_llm:
            mock_response = Mock()
            mock_response.content = "DROP TABLE dim_employees;"
            mock_llm.return_value = mock_response
            
            success, sql, error = agent.translate_to_sql("Delete all employee data")
            
            assert success is False
            assert "blocked keyword" in error.lower() or "validation failed" in error.lower()

    def test_schema_context_retrieval(self, agent):
        """Test schema context is properly retrieved."""
        query = "Show me employee salaries"
        
        with patch.object(agent.schema_index, 'get_relevant_context') as mock_context:
            mock_context.return_value = {
                "schema": [
                    {
                        "metadata": {
                            "name": "dim_employees",
                            "columns": ["employee_id", "salary", "department"]
                        },
                        "relevance_score": 0.95
                    }
                ],
                "metrics": []
            }
            
            context = agent.schema_index.get_relevant_context(query)
            
            assert "schema" in context
            assert len(context["schema"]) > 0
            assert "salary" in str(context["schema"][0]["metadata"]["columns"])

    def test_query_explanation_generation(self, agent):
        """Test SQL query explanation generation."""
        sql = "SELECT department, AVG(salary) FROM dim_employees GROUP BY department"
        
        with patch.object(agent.llm, '__call__') as mock_llm:
            mock_response = Mock()
            mock_response.content = "This query calculates the average salary by department to help understand compensation distribution across the organization."
            mock_llm.return_value = mock_response
            
            explanation = agent.get_query_explanation(sql)
            
            assert len(explanation) > 0
            assert "salary" in explanation.lower()
            assert "department" in explanation.lower()

    def test_follow_up_suggestions(self, agent):
        """Test follow-up question generation."""
        query = "Show headcount by department"
        sql = "SELECT department, COUNT(*) FROM dim_employees GROUP BY department"
        
        with patch.object(agent.llm, '__call__') as mock_llm:
            mock_response = Mock()
            mock_response.content = """How has headcount changed over time by department?
What is the average tenure in each department?
Which departments have the highest attrition rates?"""
            mock_llm.return_value = mock_response
            
            suggestions = agent.suggest_follow_up_questions(query, sql)
            
            assert len(suggestions) > 0
            assert all(isinstance(q, str) for q in suggestions)
            assert len(suggestions) <= 5

    def test_error_handling_invalid_llm_response(self, agent):
        """Test error handling when LLM returns invalid response."""
        with patch.object(agent.llm, '__call__') as mock_llm:
            mock_llm.side_effect = Exception("LLM connection failed")
            
            success, sql, error = agent.translate_to_sql("Show me data")
            
            assert success is False
            assert "error" in error.lower()

    def test_create_agent_factory(self, mock_runner):
        """Test agent factory function."""
        with patch('analytics.nl2sql.agent.Config') as mock_config:
            mock_config.LLM_PROVIDER.value = "ollama"
            mock_config.OLLAMA_MODEL = "llama3.1"
            
            with patch('analytics.nl2sql.agent.ChatOllama'):
                agent = create_agent(mock_runner)
                assert isinstance(agent, NL2SQLAgent)
                assert agent.warehouse_runner == mock_runner

    def test_attrition_query_complexity(self, agent):
        """Test complex attrition analysis query."""
        query = "Show attrition trends by department for the last 12 months"
        
        with patch.object(agent.schema_index, 'get_relevant_context') as mock_context:
            mock_context.return_value = {
                "schema": [
                    {"metadata": {"name": "fct_attrition_events", "columns": ["termination_date", "department"]}},
                    {"metadata": {"name": "dim_employees", "columns": ["employee_id", "department"]}}
                ],
                "metrics": [
                    {"metadata": {"name": "attrition_rate", "description": "Employee attrition percentage"}}
                ]
            }
            
            with patch.object(agent.llm, '__call__') as mock_llm:
                mock_response = Mock()
                mock_response.content = """SELECT 
                    DATE_TRUNC('month', termination_date) as month,
                    department,
                    COUNT(*) as terminations
                FROM marts.people.fct_attrition_events 
                WHERE termination_date >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY month, department 
                ORDER BY month, department 
                LIMIT 1000"""
                mock_llm.return_value = mock_response
                
                success, sql, error = agent.translate_to_sql(query)
                
                assert success is True
                assert "termination_date" in sql.lower()
                assert "department" in sql.lower()
                assert "12 months" in sql or "interval" in sql.lower()

    def test_multiple_table_joins(self, agent):
        """Test queries requiring multiple table joins."""
        query = "Show employee details with their manager names and region information"
        
        with patch.object(agent.schema_index, 'get_relevant_context') as mock_context:
            mock_context.return_value = {
                "schema": [
                    {
                        "metadata": {
                            "name": "dim_employees",
                            "columns": ["employee_id", "manager_id", "region_id", "full_name"]
                        }
                    }
                ],
                "metrics": []
            }
            
            with patch.object(agent.llm, '__call__') as mock_llm:
                mock_response = Mock()
                mock_response.content = """SELECT 
                    e.full_name,
                    m.full_name as manager_name,
                    r.region_name
                FROM marts.people.dim_employees e
                LEFT JOIN marts.people.dim_employees m ON e.manager_id = m.employee_id
                LEFT JOIN seeds.hr_regions r ON e.region_id = r.region_id
                WHERE e.is_active = true
                LIMIT 1000"""
                mock_llm.return_value = mock_response
                
                success, sql, error = agent.translate_to_sql(query)
                
                assert success is True
                assert "join" in sql.lower()
                assert "manager" in sql.lower()
                assert "region" in sql.lower()