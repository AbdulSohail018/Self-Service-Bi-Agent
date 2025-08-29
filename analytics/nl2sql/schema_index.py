"""Schema and metric indexing for natural language to SQL translation."""

import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import chromadb
import yaml
from chromadb.config import Settings

from app.config import Config


class SchemaIndex:
    """Vector index for schema information and business metrics."""

    def __init__(self, vector_dir: str = None):
        """Initialize schema index with vector database."""
        self.vector_dir = Path(vector_dir or Config.VECTOR_DIR)
        self.vector_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(self.vector_dir),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Get or create collections
        self.schema_collection = self.client.get_or_create_collection(
            name="schema_info",
            metadata={"description": "Database schema and table information"}
        )
        
        self.metrics_collection = self.client.get_or_create_collection(
            name="business_metrics", 
            metadata={"description": "Business metrics and KPI definitions"}
        )

    def build_index(self, warehouse_runner=None) -> None:
        """Build the complete schema index from multiple sources."""
        print("Building schema index...")
        
        # Clear existing collections
        self.client.delete_collection("schema_info")
        self.client.delete_collection("business_metrics")
        
        # Recreate collections
        self.schema_collection = self.client.get_or_create_collection("schema_info")
        self.metrics_collection = self.client.get_or_create_collection("business_metrics")
        
        # Index dbt models and documentation
        self._index_dbt_models()
        
        # Index metrics definitions
        self._index_metrics_definitions()
        
        # Index warehouse schema (if available)
        if warehouse_runner:
            self._index_warehouse_schema(warehouse_runner)
        
        # Index business glossary
        self._index_business_glossary()
        
        print("Schema index built successfully")

    def _index_dbt_models(self) -> None:
        """Index dbt model definitions and documentation."""
        dbt_models_dir = Path("dbt/models")
        
        if not dbt_models_dir.exists():
            print("dbt models directory not found, skipping dbt indexing")
            return
        
        # Find all SQL model files
        model_files = list(dbt_models_dir.rglob("*.sql"))
        
        documents = []
        metadatas = []
        ids = []
        
        for model_file in model_files:
            # Read model content
            content = model_file.read_text()
            
            # Extract model info
            model_name = model_file.stem
            schema_path = str(model_file.relative_to(dbt_models_dir))
            
            # Parse SQL for table and column references
            table_info = self._parse_sql_for_schema_info(content, model_name)
            
            # Create document for indexing
            doc = f"""
            Model: {model_name}
            Path: {schema_path}
            Description: {table_info.get('description', 'dbt model')}
            Tables: {', '.join(table_info.get('tables', []))}
            Columns: {', '.join(table_info.get('columns', []))}
            Content: {content[:500]}...
            """
            
            documents.append(doc)
            metadatas.append({
                "type": "dbt_model",
                "name": model_name,
                "path": schema_path,
                "tables": table_info.get('tables', []),
                "columns": table_info.get('columns', [])
            })
            ids.append(f"dbt_model_{model_name}")
        
        if documents:
            self.schema_collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def _index_metrics_definitions(self) -> None:
        """Index business metrics from metrics.yml files."""
        metrics_files = list(Path("dbt/models").rglob("metrics.yml"))
        
        documents = []
        metadatas = []
        ids = []
        
        for metrics_file in metrics_files:
            try:
                with open(metrics_file, 'r') as f:
                    metrics_data = yaml.safe_load(f)
                
                if 'metrics' in metrics_data:
                    for metric in metrics_data['metrics']:
                        metric_name = metric.get('name', 'unknown')
                        description = metric.get('description', '')
                        calculation = metric.get('calculation_method', '')
                        expression = metric.get('expression', '')
                        
                        # Create searchable document
                        doc = f"""
                        Metric: {metric_name}
                        Description: {description}
                        Calculation: {calculation}
                        Expression: {expression}
                        Business Context: HR analytics, employee metrics, attrition
                        """
                        
                        documents.append(doc)
                        metadatas.append({
                            "type": "business_metric",
                            "name": metric_name,
                            "description": description,
                            "calculation": calculation,
                            "expression": expression
                        })
                        ids.append(f"metric_{metric_name}")
                        
            except Exception as e:
                print(f"Error processing metrics file {metrics_file}: {e}")
        
        if documents:
            self.metrics_collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def _index_warehouse_schema(self, warehouse_runner) -> None:
        """Index live warehouse schema information."""
        try:
            # Get schema information from warehouse
            schema_info = warehouse_runner.get_schema_info()
            
            documents = []
            metadatas = []
            ids = []
            
            for table_name, columns in schema_info.items():
                # Create document for each table
                column_names = [col['name'] for col in columns]
                column_types = [f"{col['name']} ({col['type']})" for col in columns]
                
                doc = f"""
                Table: {table_name}
                Columns: {', '.join(column_names)}
                Schema: {', '.join(column_types)}
                Source: Live warehouse schema
                """
                
                documents.append(doc)
                metadatas.append({
                    "type": "warehouse_table",
                    "name": table_name,
                    "columns": column_names,
                    "column_types": {col['name']: col['type'] for col in columns}
                })
                ids.append(f"warehouse_table_{table_name}")
            
            if documents:
                self.schema_collection.add(
                    documents=documents,
                    metadatas=metadatas,
                    ids=ids
                )
                
        except Exception as e:
            print(f"Error indexing warehouse schema: {e}")

    def _index_business_glossary(self) -> None:
        """Index business glossary and metric dictionary."""
        glossary_files = [
            "docs/metric_dictionary.md",
            "docs/glossary.md"
        ]
        
        documents = []
        metadatas = []
        ids = []
        
        for file_path in glossary_files:
            if Path(file_path).exists():
                content = Path(file_path).read_text()
                
                # Split content into sections
                sections = self._split_markdown_content(content)
                
                for i, section in enumerate(sections):
                    documents.append(section['content'])
                    metadatas.append({
                        "type": "business_glossary",
                        "source": file_path,
                        "section": section['title'],
                        "category": "business_definition"
                    })
                    ids.append(f"glossary_{Path(file_path).stem}_{i}")
        
        if documents:
            self.schema_collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def search_schema(self, query: str, n_results: int = 5) -> List[Dict]:
        """Search schema information for relevant tables and columns."""
        results = self.schema_collection.query(
            query_texts=[query],
            n_results=n_results
        )
        
        schema_info = []
        if results['documents']:
            for i, doc in enumerate(results['documents'][0]):
                metadata = results['metadatas'][0][i]
                distance = results['distances'][0][i]
                
                schema_info.append({
                    "content": doc,
                    "metadata": metadata,
                    "relevance_score": 1 - distance  # Convert distance to similarity
                })
        
        return schema_info

    def search_metrics(self, query: str, n_results: int = 3) -> List[Dict]:
        """Search business metrics for relevant KPIs and calculations."""
        results = self.metrics_collection.query(
            query_texts=[query],
            n_results=n_results
        )
        
        metrics_info = []
        if results['documents']:
            for i, doc in enumerate(results['documents'][0]):
                metadata = results['metadatas'][0][i]
                distance = results['distances'][0][i]
                
                metrics_info.append({
                    "content": doc,
                    "metadata": metadata,
                    "relevance_score": 1 - distance
                })
        
        return metrics_info

    def get_relevant_context(self, query: str) -> Dict[str, List[Dict]]:
        """Get both schema and metrics context for a query."""
        return {
            "schema": self.search_schema(query),
            "metrics": self.search_metrics(query)
        }

    def _parse_sql_for_schema_info(self, sql_content: str, model_name: str) -> Dict:
        """Extract table and column information from SQL content."""
        # Simple parsing - could be enhanced with sqlglot
        tables = []
        columns = []
        
        # Extract FROM and JOIN clauses
        import re
        
        # Find table references
        table_patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, sql_content, re.IGNORECASE)
            tables.extend(matches)
        
        # Find column references (basic extraction)
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_content, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            # Simple column extraction
            column_matches = re.findall(r'([a-zA-Z_][a-zA-Z0-9_.]*)', select_clause)
            columns.extend(column_matches)
        
        return {
            "description": f"Data model for {model_name}",
            "tables": list(set(tables)),
            "columns": list(set(columns))
        }

    def _split_markdown_content(self, content: str) -> List[Dict]:
        """Split markdown content into sections."""
        sections = []
        current_section = {"title": "Introduction", "content": ""}
        
        lines = content.split('\n')
        for line in lines:
            if line.startswith('#'):
                # New section
                if current_section["content"].strip():
                    sections.append(current_section)
                
                current_section = {
                    "title": line.lstrip('#').strip(),
                    "content": ""
                }
            else:
                current_section["content"] += line + "\n"
        
        # Add final section
        if current_section["content"].strip():
            sections.append(current_section)
        
        return sections