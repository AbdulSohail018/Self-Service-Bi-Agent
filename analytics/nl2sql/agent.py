"""Natural Language to SQL Agent with schema awareness."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.chat_models import ChatOllama, ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import HumanMessage, SystemMessage
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

from app.config import Config
from analytics.nl2sql.guardrails import SQLGuardrails
from analytics.nl2sql.schema_index import SchemaIndex


class SchemaSearchInput(BaseModel):
    """Input for schema search tool."""
    query: str = Field(description="Search query for schema information")
    max_results: int = Field(default=5, description="Maximum number of results")


class SchemaSearchTool(BaseTool):
    """Tool for searching schema and table information."""
    
    name = "schema_search"
    description = "Search for relevant database tables, columns, and schema information"
    args_schema = SchemaSearchInput
    
    def __init__(self, schema_index: SchemaIndex):
        super().__init__()
        self.schema_index = schema_index
    
    def _run(self, query: str, max_results: int = 5) -> str:
        """Search schema information."""
        context = self.schema_index.get_relevant_context(query)
        
        result = "## Relevant Schema Information:\n\n"
        
        # Add schema results
        if context["schema"]:
            result += "### Tables and Columns:\n"
            for item in context["schema"][:max_results]:
                metadata = item["metadata"]
                result += f"- **{metadata.get('name', 'Unknown')}** "
                result += f"(Type: {metadata.get('type', 'unknown')})\n"
                if metadata.get('columns'):
                    result += f"  Columns: {', '.join(metadata['columns'][:10])}\n"
                result += f"  Relevance: {item['relevance_score']:.2f}\n\n"
        
        # Add metrics results
        if context["metrics"]:
            result += "### Business Metrics:\n"
            for item in context["metrics"][:3]:
                metadata = item["metadata"]
                result += f"- **{metadata.get('name', 'Unknown')}**: "
                result += f"{metadata.get('description', 'No description')}\n"
                if metadata.get('expression'):
                    result += f"  Expression: {metadata['expression']}\n"
                result += f"  Relevance: {item['relevance_score']:.2f}\n\n"
        
        return result


class MetricsSearchInput(BaseModel):
    """Input for metrics search tool."""
    query: str = Field(description="Search query for business metrics")


class MetricsSearchTool(BaseTool):
    """Tool for searching business metrics and KPI definitions."""
    
    name = "metrics_search"
    description = "Search for business metrics, KPIs, and calculation definitions"
    args_schema = MetricsSearchInput
    
    def __init__(self, schema_index: SchemaIndex):
        super().__init__()
        self.schema_index = schema_index
    
    def _run(self, query: str) -> str:
        """Search metrics information."""
        metrics = self.schema_index.search_metrics(query, n_results=5)
        
        if not metrics:
            return "No relevant metrics found."
        
        result = "## Relevant Business Metrics:\n\n"
        for metric in metrics:
            metadata = metric["metadata"]
            result += f"### {metadata.get('name', 'Unknown Metric')}\n"
            result += f"**Description**: {metadata.get('description', 'No description')}\n"
            result += f"**Calculation**: {metadata.get('calculation', 'Not specified')}\n"
            if metadata.get('expression'):
                result += f"**Expression**: `{metadata['expression']}`\n"
            result += f"**Relevance**: {metric['relevance_score']:.2f}\n\n"
        
        return result


class NL2SQLAgent:
    """Natural Language to SQL translation agent."""

    def __init__(self, warehouse_runner=None):
        """Initialize the NL2SQL agent."""
        self.config = Config
        self.warehouse_runner = warehouse_runner
        self.guardrails = SQLGuardrails()
        
        # Initialize schema index
        self.schema_index = SchemaIndex()
        
        # Initialize LLM
        self.llm = self._init_llm()
        
        # Load few-shot examples
        self.examples = self._load_examples()
        
        # Create tools
        self.tools = [
            SchemaSearchTool(self.schema_index),
            MetricsSearchTool(self.schema_index)
        ]

    def _init_llm(self):
        """Initialize the language model."""
        if self.config.LLM_PROVIDER.value == "openai":
            if not self.config.OPENAI_API_KEY:
                raise ValueError("OpenAI API key is required")
            return ChatOpenAI(
                model="gpt-4",
                temperature=0.1,
                openai_api_key=self.config.OPENAI_API_KEY
            )
        else:
            # Default to Ollama
            return ChatOllama(
                model=self.config.OLLAMA_MODEL,
                temperature=0.1,
                base_url="http://localhost:11434"
            )

    def _load_examples(self) -> List[Dict]:
        """Load few-shot examples from YAML file."""
        examples_file = Path("analytics/nl2sql/prompts/examples.yml")
        
        if examples_file.exists():
            with open(examples_file, 'r') as f:
                data = yaml.safe_load(f)
                return data.get('examples', [])
        
        return []

    def _get_system_prompt(self) -> str:
        """Get the system prompt for SQL generation."""
        system_prompt_file = Path("analytics/nl2sql/prompts/system.sql")
        
        if system_prompt_file.exists():
            return system_prompt_file.read_text()
        
        # Fallback system prompt
        return """You are an expert SQL analyst specializing in HR analytics.
        Convert natural language questions into accurate SQL queries.
        Use only SELECT statements and include appropriate LIMIT clauses.
        Focus on business-relevant insights from employee and attrition data."""

    def _build_few_shot_examples(self) -> str:
        """Build few-shot examples string."""
        if not self.examples:
            return ""
        
        examples_text = "\n## Example Queries:\n\n"
        for i, example in enumerate(self.examples[:5], 1):
            examples_text += f"**Example {i}:**\n"
            examples_text += f"Question: {example['question']}\n"
            examples_text += f"SQL:\n```sql\n{example['sql']}\n```\n\n"
        
        return examples_text

    def translate_to_sql(self, natural_language_query: str) -> Tuple[bool, str, str]:
        """
        Translate natural language query to SQL.
        
        Returns:
            Tuple of (success, sql_query, error_message)
        """
        try:
            # Get schema context
            schema_context = self.schema_index.get_relevant_context(natural_language_query)
            
            # Build context for the LLM
            context = self._build_context(natural_language_query, schema_context)
            
            # Generate SQL using LLM
            sql_query = self._generate_sql_with_llm(natural_language_query, context)
            
            # Validate SQL with guardrails
            is_valid, error, cleaned_sql = self.guardrails.validate_sql(sql_query)
            
            if not is_valid:
                return False, "", f"SQL validation failed: {error}"
            
            return True, cleaned_sql, ""
            
        except Exception as e:
            return False, "", f"Error generating SQL: {str(e)}"

    def _build_context(self, query: str, schema_context: Dict) -> str:
        """Build context string for LLM."""
        context = "## Available Schema and Metrics:\n\n"
        
        # Add schema information
        if schema_context.get("schema"):
            context += "### Database Tables:\n"
            for item in schema_context["schema"][:5]:
                metadata = item["metadata"]
                context += f"- **{metadata.get('name', 'Unknown')}**\n"
                if metadata.get("columns"):
                    columns = metadata["columns"][:8]  # Limit columns shown
                    context += f"  Columns: {', '.join(columns)}\n"
                    if len(metadata["columns"]) > 8:
                        context += f"  ... and {len(metadata['columns']) - 8} more\n"
                context += "\n"
        
        # Add metrics information
        if schema_context.get("metrics"):
            context += "### Business Metrics:\n"
            for item in schema_context["metrics"][:3]:
                metadata = item["metadata"]
                context += f"- **{metadata.get('name', 'Unknown')}**: {metadata.get('description', '')}\n"
                if metadata.get("expression"):
                    context += f"  Expression: `{metadata['expression']}`\n"
                context += "\n"
        
        # Add few-shot examples
        context += self._build_few_shot_examples()
        
        return context

    def _generate_sql_with_llm(self, query: str, context: str) -> str:
        """Generate SQL using the language model."""
        system_prompt = self._get_system_prompt()
        
        # Create the full prompt
        full_prompt = f"""{system_prompt}

{context}

## Task:
Convert the following natural language query into a SQL query:

Query: "{query}"

Return only the SQL query without any explanations or markdown formatting."""

        # Generate response
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"{context}\n\nQuery: {query}\n\nReturn only the SQL query:")
        ]
        
        response = self.llm(messages)
        
        # Extract SQL from response
        sql = response.content.strip()
        
        # Clean up response (remove markdown if present)
        if sql.startswith("```sql"):
            sql = sql.replace("```sql", "").replace("```", "").strip()
        elif sql.startswith("```"):
            sql = sql.replace("```", "").strip()
        
        return sql

    def rebuild_schema_index(self) -> None:
        """Rebuild the schema index from current sources."""
        print("Rebuilding schema index...")
        self.schema_index.build_index(self.warehouse_runner)
        print("Schema index rebuilt successfully")

    def get_query_explanation(self, sql: str) -> str:
        """Get a natural language explanation of the SQL query."""
        try:
            prompt = f"""Explain this SQL query in simple business terms:

```sql
{sql}
```

Focus on:
1. What business question it answers
2. What data it analyzes
3. Key metrics or insights it provides

Keep the explanation concise and business-focused."""

            response = self.llm([HumanMessage(content=prompt)])
            return response.content.strip()
            
        except Exception as e:
            return f"Could not generate explanation: {str(e)}"

    def suggest_follow_up_questions(self, query: str, sql: str) -> List[str]:
        """Suggest relevant follow-up questions based on the current query."""
        try:
            prompt = f"""Based on this business question and SQL query, suggest 3-5 relevant follow-up questions that a business user might want to explore:

Original Question: "{query}"
SQL Query: {sql}

Suggest specific, actionable follow-up questions that would provide additional business insights. Focus on:
- Drilling down into specific dimensions (time, region, department, etc.)
- Comparing different segments or periods
- Identifying trends or patterns
- Exploring root causes

Return only the questions, one per line, without numbering."""

            response = self.llm([HumanMessage(content=prompt)])
            questions = response.content.strip().split('\n')
            
            # Clean and filter questions
            follow_ups = []
            for q in questions:
                q = q.strip().lstrip('-•').strip()
                if q and len(q) > 10:  # Basic filtering
                    follow_ups.append(q)
            
            return follow_ups[:5]  # Limit to 5 suggestions
            
        except Exception as e:
            print(f"Error generating follow-up questions: {e}")
            return [
                "What trends do you see over time?",
                "How does this vary by region or department?",
                "What factors might be driving these results?"
            ]


def create_agent(warehouse_runner=None) -> NL2SQLAgent:
    """Factory function to create a configured NL2SQL agent."""
    agent = NL2SQLAgent(warehouse_runner)
    
    # Build initial schema index if it doesn't exist
    try:
        # Check if index exists by trying a simple search
        agent.schema_index.search_schema("test", n_results=1)
    except:
        # Index doesn't exist or is empty, build it
        agent.rebuild_schema_index()
    
    return agent