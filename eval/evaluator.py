"""Evaluation harness for NL→SQL translation quality."""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
import sqlite3
from datetime import datetime

import pandas as pd
import sqlglot
from sqlglot import parse_one, transpile

from analytics.nl2sql.agent import create_agent
from analytics.runners.duckdb_runner import DuckDBRunner
from analytics.nl2sql.guardrails import SQLGuardrails


class NL2SQLEvaluator:
    """Evaluate NL→SQL translation quality against test cases."""

    def __init__(self, test_cases_file: str = "eval/cases.yml"):
        """Initialize evaluator with test cases."""
        self.test_cases_file = Path(test_cases_file)
        self.test_cases = self._load_test_cases()
        self.results_db = Path("eval/results.db")
        self._init_results_db()

    def _load_test_cases(self) -> List[Dict]:
        """Load test cases from YAML file."""
        if not self.test_cases_file.exists():
            raise FileNotFoundError(f"Test cases file not found: {self.test_cases_file}")
        
        with open(self.test_cases_file, 'r') as f:
            data = yaml.safe_load(f)
        
        return data.get('test_cases', [])

    def _init_results_db(self):
        """Initialize SQLite database for storing evaluation results."""
        self.results_db.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(self.results_db)
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS evaluation_runs (
            run_id TEXT PRIMARY KEY,
            timestamp TEXT,
            total_cases INTEGER,
            passed_cases INTEGER,
            failed_cases INTEGER,
            overall_score REAL,
            notes TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS case_results (
            run_id TEXT,
            case_id TEXT,
            question TEXT,
            generated_sql TEXT,
            expected_sql TEXT,
            execution_success BOOLEAN,
            sql_similarity_score REAL,
            schema_compliance_score REAL,
            result_accuracy_score REAL,
            overall_case_score REAL,
            error_message TEXT,
            execution_time_ms INTEGER,
            FOREIGN KEY (run_id) REFERENCES evaluation_runs (run_id)
        )
        """)
        
        conn.commit()
        conn.close()

    def run_evaluation(self, run_id: str = None) -> Dict:
        """Run complete evaluation suite."""
        if not run_id:
            run_id = f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"🔬 Starting evaluation run: {run_id}")
        print(f"📋 Total test cases: {len(self.test_cases)}")
        
        # Initialize components
        try:
            db_runner = DuckDBRunner()
            agent = create_agent(db_runner)
        except Exception as e:
            print(f"❌ Failed to initialize components: {e}")
            return {"error": str(e)}
        
        results = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "total_cases": len(self.test_cases),
            "case_results": [],
            "passed_cases": 0,
            "failed_cases": 0,
            "overall_score": 0.0
        }
        
        # Run each test case
        for i, test_case in enumerate(self.test_cases, 1):
            print(f"\n📝 Running case {i}/{len(self.test_cases)}: {test_case['id']}")
            
            case_result = self._evaluate_single_case(test_case, agent, db_runner)
            results["case_results"].append(case_result)
            
            if case_result["overall_case_score"] >= 0.7:  # 70% threshold for pass
                results["passed_cases"] += 1
                print(f"✅ PASSED (Score: {case_result['overall_case_score']:.2f})")
            else:
                results["failed_cases"] += 1
                print(f"❌ FAILED (Score: {case_result['overall_case_score']:.2f})")
                if case_result.get("error_message"):
                    print(f"   Error: {case_result['error_message']}")
        
        # Calculate overall score
        if results["case_results"]:
            results["overall_score"] = sum(
                case["overall_case_score"] for case in results["case_results"]
            ) / len(results["case_results"])
        
        # Save results
        self._save_results(results)
        
        # Print summary
        self._print_summary(results)
        
        return results

    def _evaluate_single_case(self, test_case: Dict, agent, db_runner) -> Dict:
        """Evaluate a single test case."""
        case_id = test_case["id"]
        question = test_case["question"]
        
        result = {
            "case_id": case_id,
            "question": question,
            "generated_sql": "",
            "expected_sql": "",
            "execution_success": False,
            "sql_similarity_score": 0.0,
            "schema_compliance_score": 0.0,
            "result_accuracy_score": 0.0,
            "overall_case_score": 0.0,
            "error_message": "",
            "execution_time_ms": 0
        }
        
        start_time = datetime.now()
        
        try:
            # Generate SQL
            success, generated_sql, error = agent.translate_to_sql(question)
            
            if not success:
                result["error_message"] = error
                return result
            
            result["generated_sql"] = generated_sql
            
            # Load expected SQL if available
            expected_sql_file = Path(f"eval/fixtures/expected_sql/{case_id}.sql")
            if expected_sql_file.exists():
                result["expected_sql"] = expected_sql_file.read_text().strip()
            
            # Evaluate SQL quality
            result["schema_compliance_score"] = self._evaluate_schema_compliance(
                generated_sql, test_case
            )
            
            if result["expected_sql"]:
                result["sql_similarity_score"] = self._evaluate_sql_similarity(
                    generated_sql, result["expected_sql"]
                )
            
            # Execute SQL and evaluate results
            try:
                df, metadata = db_runner.execute_query(generated_sql)
                result["execution_success"] = True
                result["result_accuracy_score"] = self._evaluate_result_accuracy(
                    df, test_case
                )
            except Exception as e:
                result["error_message"] = f"Execution failed: {str(e)}"
            
            # Calculate overall score
            weights = {
                "schema_compliance": 0.3,
                "sql_similarity": 0.3,
                "result_accuracy": 0.4
            }
            
            result["overall_case_score"] = (
                weights["schema_compliance"] * result["schema_compliance_score"] +
                weights["sql_similarity"] * result["sql_similarity_score"] +
                weights["result_accuracy"] * result["result_accuracy_score"]
            )
            
        except Exception as e:
            result["error_message"] = f"Evaluation error: {str(e)}"
        
        finally:
            end_time = datetime.now()
            result["execution_time_ms"] = int((end_time - start_time).total_seconds() * 1000)
        
        return result

    def _evaluate_schema_compliance(self, sql: str, test_case: Dict) -> float:
        """Evaluate SQL compliance with schema expectations."""
        score = 0.0
        max_score = 100.0
        
        try:
            # Parse SQL
            parsed = parse_one(sql, dialect="duckdb")
            
            # Check for expected tables
            expected_tables = test_case.get("expected_tables", [])
            if expected_tables:
                found_tables = self._extract_table_references(sql)
                table_matches = sum(
                    1 for table in expected_tables 
                    if any(table in found_table for found_table in found_tables)
                )
                score += (table_matches / len(expected_tables)) * 30
            
            # Check for expected aggregations
            expected_aggs = test_case.get("expected_aggregations", [])
            if expected_aggs:
                sql_upper = sql.upper()
                agg_matches = sum(1 for agg in expected_aggs if agg in sql_upper)
                score += (agg_matches / len(expected_aggs)) * 20
            
            # Check for filters
            expected_filters = test_case.get("expected_filters", [])
            if expected_filters:
                filter_matches = sum(
                    1 for filter_col in expected_filters 
                    if filter_col.lower() in sql.lower()
                )
                score += (filter_matches / len(expected_filters)) * 25
            
            # SQL safety checks
            guardrails = SQLGuardrails()
            is_valid, error, _ = guardrails.validate_sql(sql)
            if is_valid:
                score += 25
            
        except Exception as e:
            print(f"Schema compliance evaluation error: {e}")
        
        return min(score / max_score, 1.0)

    def _evaluate_sql_similarity(self, generated_sql: str, expected_sql: str) -> float:
        """Evaluate similarity between generated and expected SQL."""
        try:
            # Normalize both SQL queries
            gen_normalized = self._normalize_sql(generated_sql)
            exp_normalized = self._normalize_sql(expected_sql)
            
            # Simple similarity based on common keywords and structure
            gen_tokens = set(gen_normalized.split())
            exp_tokens = set(exp_normalized.split())
            
            if not exp_tokens:
                return 0.0
            
            intersection = gen_tokens.intersection(exp_tokens)
            similarity = len(intersection) / len(exp_tokens)
            
            return min(similarity, 1.0)
            
        except Exception as e:
            print(f"SQL similarity evaluation error: {e}")
            return 0.0

    def _evaluate_result_accuracy(self, df: pd.DataFrame, test_case: Dict) -> float:
        """Evaluate accuracy of query results."""
        score = 0.0
        
        # Basic result validation
        if df.empty:
            return 0.0
        
        # Check for expected columns
        expected_columns = test_case.get("expected_columns", [])
        if expected_columns:
            df_columns_lower = [col.lower() for col in df.columns]
            column_matches = sum(
                1 for col in expected_columns 
                if any(col.lower() in df_col for df_col in df_columns_lower)
            )
            score += (column_matches / len(expected_columns)) * 0.6
        
        # Check result reasonableness
        if len(df) > 0:
            score += 0.2  # Non-empty result
        
        if len(df.columns) >= 2:
            score += 0.2  # Multiple columns (indicates proper joins/grouping)
        
        return min(score, 1.0)

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison."""
        try:
            # Parse and format consistently
            parsed = parse_one(sql, dialect="duckdb")
            normalized = parsed.sql(dialect="duckdb", pretty=True)
            return normalized.upper()
        except:
            # Fallback to simple normalization
            return " ".join(sql.upper().split())

    def _extract_table_references(self, sql: str) -> List[str]:
        """Extract table references from SQL."""
        try:
            parsed = parse_one(sql, dialect="duckdb")
            tables = []
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
        except:
            return []

    def _save_results(self, results: Dict):
        """Save evaluation results to database."""
        conn = sqlite3.connect(self.results_db)
        cursor = conn.cursor()
        
        # Save run summary
        cursor.execute("""
        INSERT INTO evaluation_runs 
        (run_id, timestamp, total_cases, passed_cases, failed_cases, overall_score, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            results["run_id"],
            results["timestamp"],
            results["total_cases"],
            results["passed_cases"],
            results["failed_cases"],
            results["overall_score"],
            f"Automated evaluation run"
        ))
        
        # Save individual case results
        for case_result in results["case_results"]:
            cursor.execute("""
            INSERT INTO case_results 
            (run_id, case_id, question, generated_sql, expected_sql, execution_success,
             sql_similarity_score, schema_compliance_score, result_accuracy_score,
             overall_case_score, error_message, execution_time_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                results["run_id"],
                case_result["case_id"],
                case_result["question"],
                case_result["generated_sql"],
                case_result["expected_sql"],
                case_result["execution_success"],
                case_result["sql_similarity_score"],
                case_result["schema_compliance_score"],
                case_result["result_accuracy_score"],
                case_result["overall_case_score"],
                case_result["error_message"],
                case_result["execution_time_ms"]
            ))
        
        conn.commit()
        conn.close()

    def _print_summary(self, results: Dict):
        """Print evaluation summary."""
        print("\n" + "="*60)
        print("📊 EVALUATION SUMMARY")
        print("="*60)
        print(f"Run ID: {results['run_id']}")
        print(f"Total Cases: {results['total_cases']}")
        print(f"Passed: {results['passed_cases']} ({results['passed_cases']/results['total_cases']*100:.1f}%)")
        print(f"Failed: {results['failed_cases']} ({results['failed_cases']/results['total_cases']*100:.1f}%)")
        print(f"Overall Score: {results['overall_score']:.3f}")
        
        # Category breakdown
        categories = {}
        for case_result in results["case_results"]:
            case_id = case_result["case_id"]
            test_case = next((tc for tc in self.test_cases if tc["id"] == case_id), {})
            category = test_case.get("category", "unknown")
            
            if category not in categories:
                categories[category] = {"total": 0, "passed": 0, "avg_score": 0}
            
            categories[category]["total"] += 1
            categories[category]["avg_score"] += case_result["overall_case_score"]
            if case_result["overall_case_score"] >= 0.7:
                categories[category]["passed"] += 1
        
        if categories:
            print("\n📈 CATEGORY BREAKDOWN:")
            for category, stats in categories.items():
                avg_score = stats["avg_score"] / stats["total"]
                pass_rate = stats["passed"] / stats["total"] * 100
                print(f"  {category}: {stats['passed']}/{stats['total']} ({pass_rate:.1f}%) - Avg Score: {avg_score:.3f}")
        
        print("="*60)

    def get_historical_results(self, limit: int = 10) -> List[Dict]:
        """Get historical evaluation results."""
        conn = sqlite3.connect(self.results_db)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT * FROM evaluation_runs 
        ORDER BY timestamp DESC 
        LIMIT ?
        """, (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results


def main():
    """Main evaluation script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Evaluate NL→SQL translation quality")
    parser.add_argument("--run-id", help="Custom run ID")
    parser.add_argument("--cases-file", default="eval/cases.yml", help="Test cases file")
    parser.add_argument("--history", action="store_true", help="Show historical results")
    
    args = parser.parse_args()
    
    evaluator = NL2SQLEvaluator(args.cases_file)
    
    if args.history:
        print("📊 Historical Results:")
        for result in evaluator.get_historical_results():
            print(f"  {result['run_id']}: {result['overall_score']:.3f} ({result['passed_cases']}/{result['total_cases']} passed)")
    else:
        evaluator.run_evaluation(args.run_id)


if __name__ == "__main__":
    main()