"""Follow-up insights and suggestion generation."""

from typing import Dict, List, Optional, Tuple

import pandas as pd
from langchain.chat_models import ChatOllama, ChatOpenAI
from langchain.schema import HumanMessage

from app.config import Config


class InsightGenerator:
    """Generate business insights and follow-up suggestions from query results."""

    def __init__(self):
        """Initialize insight generator."""
        self.config = Config
        self.llm = self._init_llm()

    def _init_llm(self):
        """Initialize the language model."""
        if self.config.LLM_PROVIDER.value == "openai":
            if not self.config.OPENAI_API_KEY:
                return None  # Graceful fallback
            return ChatOpenAI(
                model="gpt-3.5-turbo",
                temperature=0.3,
                openai_api_key=self.config.OPENAI_API_KEY
            )
        else:
            try:
                return ChatOllama(
                    model=self.config.OLLAMA_MODEL,
                    temperature=0.3,
                    base_url="http://localhost:11434"
                )
            except:
                return None  # Graceful fallback if Ollama not available

    def generate_narrative(self, df: pd.DataFrame, query: str, sql: str, metadata: Dict = None) -> str:
        """
        Generate a narrative explanation of the query results.
        
        Args:
            df: Query results DataFrame
            query: Original natural language query
            sql: Generated SQL query
            metadata: Query execution metadata
            
        Returns:
            Narrative explanation string
        """
        if df.empty:
            return "No data was found matching your query criteria."

        # Generate basic insights without LLM if needed
        basic_insights = self._generate_basic_insights(df, query)
        
        if not self.llm:
            return basic_insights

        try:
            # Create data summary for LLM
            data_summary = self._create_data_summary(df, query, sql)
            
            prompt = f"""Based on the following business query and data results, provide a concise narrative explanation of what the data shows. Focus on key insights, trends, and business implications.

Original Question: "{query}"

Data Summary:
{data_summary}

Generate a 2-3 sentence business-focused narrative that explains:
1. What the data shows
2. Key patterns or insights
3. Business implications (if apparent)

Keep it concise and business-friendly. Do not mention technical details like SQL or data processing."""

            response = self.llm([HumanMessage(content=prompt)])
            return response.content.strip()
            
        except Exception as e:
            print(f"Error generating narrative: {e}")
            return basic_insights

    def generate_follow_up_questions(self, df: pd.DataFrame, query: str, sql: str) -> List[str]:
        """
        Generate relevant follow-up questions based on the query and results.
        
        Args:
            df: Query results DataFrame
            query: Original natural language query
            sql: Generated SQL query
            
        Returns:
            List of follow-up question strings
        """
        if df.empty:
            return self._get_fallback_questions()

        # Generate rule-based suggestions
        rule_based = self._generate_rule_based_suggestions(df, query)
        
        if not self.llm:
            return rule_based

        try:
            data_summary = self._create_data_summary(df, query, sql)
            
            prompt = f"""Based on this business analysis, suggest 4-5 specific follow-up questions that would provide additional valuable insights.

Original Question: "{query}"
Data Summary: {data_summary}

Generate follow-up questions that:
1. Drill down into specific dimensions (time, region, department, etc.)
2. Compare different segments or periods  
3. Explore potential root causes
4. Identify actionable next steps

Return only the questions, one per line, without numbering or bullets. Make them specific and actionable."""

            response = self.llm([HumanMessage(content=prompt)])
            questions = [q.strip() for q in response.content.strip().split('\n') if q.strip()]
            
            # Combine LLM and rule-based suggestions
            all_questions = questions + rule_based
            return list(dict.fromkeys(all_questions))[:5]  # Remove duplicates, limit to 5
            
        except Exception as e:
            print(f"Error generating follow-up questions: {e}")
            return rule_based

    def _generate_basic_insights(self, df: pd.DataFrame, query: str) -> str:
        """Generate basic insights without LLM."""
        insights = []
        
        row_count = len(df)
        col_count = len(df.columns)
        
        insights.append(f"Found {row_count:,} records with {col_count} attributes.")
        
        # Analyze numeric columns for basic insights
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        for col in numeric_cols[:2]:  # Limit to first 2 numeric columns
            if col in df.columns:
                mean_val = df[col].mean()
                max_val = df[col].max()
                min_val = df[col].min()
                
                if pd.notna(mean_val):
                    insights.append(f"Average {col.replace('_', ' ')}: {mean_val:.1f} (range: {min_val:.1f} - {max_val:.1f})")
        
        return " ".join(insights)

    def _create_data_summary(self, df: pd.DataFrame, query: str, sql: str) -> str:
        """Create a concise summary of the data for LLM processing."""
        summary_parts = []
        
        # Basic stats
        summary_parts.append(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Column info
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        
        if numeric_cols:
            summary_parts.append(f"Numeric columns: {', '.join(numeric_cols[:3])}")
        if categorical_cols:
            summary_parts.append(f"Categories: {', '.join(categorical_cols[:3])}")
        if date_cols:
            summary_parts.append(f"Date columns: {', '.join(date_cols[:2])}")
        
        # Sample values for key columns
        if len(df) > 0:
            key_stats = []
            for col in numeric_cols[:2]:
                if col in df.columns:
                    mean_val = df[col].mean()
                    if pd.notna(mean_val):
                        key_stats.append(f"{col}: avg {mean_val:.1f}")
            
            if key_stats:
                summary_parts.append(f"Key metrics: {', '.join(key_stats)}")
        
        return " | ".join(summary_parts)

    def _generate_rule_based_suggestions(self, df: pd.DataFrame, query: str) -> List[str]:
        """Generate follow-up suggestions using business rules."""
        suggestions = []
        
        # Analyze query patterns
        query_lower = query.lower()
        
        # Time-based suggestions
        if any(word in query_lower for word in ['trend', 'time', 'month', 'quarter', 'year']):
            suggestions.append("How do these trends compare to the previous period?")
            suggestions.append("What seasonal patterns exist in this data?")
        
        # Regional/departmental analysis
        if any(word in query_lower for word in ['region', 'department', 'location']):
            suggestions.append("Which specific regions drive these results?")
            suggestions.append("How do department-level metrics compare?")
        
        # Attrition-specific suggestions
        if any(word in query_lower for word in ['attrition', 'turnover', 'retention', 'leaving']):
            suggestions.append("What are the top reasons for employee departures?")
            suggestions.append("How does attrition vary by tenure and salary band?")
            suggestions.append("Which departments have the highest retention risk?")
        
        # Headcount/hiring suggestions
        if any(word in query_lower for word in ['headcount', 'hire', 'employee count', 'workforce']):
            suggestions.append("What is the current hiring velocity trend?")
            suggestions.append("How does workforce distribution look across regions?")
        
        # Performance/salary analysis
        if any(word in query_lower for word in ['salary', 'compensation', 'performance']):
            suggestions.append("How do compensation levels compare by role and region?")
            suggestions.append("What factors correlate with higher performance ratings?")
        
        # General analytical suggestions
        suggestions.extend([
            "What external factors might influence these patterns?",
            "How can we benchmark these results against industry standards?"
        ])
        
        # Return unique suggestions, limited to 3
        return list(dict.fromkeys(suggestions))[:3]

    def _get_fallback_questions(self) -> List[str]:
        """Get fallback questions when no data is returned."""
        return [
            "What time period should we analyze instead?",
            "Are there alternative metrics we should consider?",
            "Should we examine different organizational segments?",
            "What data quality issues might be affecting results?"
        ]

    def generate_key_insights(self, df: pd.DataFrame, chart_type: str = None) -> List[Dict[str, str]]:
        """
        Generate key insights from the data for display in UI.
        
        Args:
            df: Query results DataFrame
            chart_type: Type of chart being displayed
            
        Returns:
            List of insight dictionaries with 'type', 'title', and 'value' keys
        """
        insights = []
        
        if df.empty:
            return insights
        
        # Basic data insights
        insights.append({
            "type": "info",
            "title": "Dataset Size",
            "value": f"{len(df):,} rows, {len(df.columns)} columns"
        })
        
        # Numeric insights
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if numeric_cols and chart_type == 'line':
            # Trend analysis for line charts
            for col in numeric_cols[:1]:  # Focus on first numeric column
                if len(df) > 1:
                    first_val = df[col].iloc[0]
                    last_val = df[col].iloc[-1]
                    
                    if pd.notna(first_val) and pd.notna(last_val) and first_val != 0:
                        change_pct = ((last_val - first_val) / first_val) * 100
                        trend_type = "success" if change_pct > 0 else "warning"
                        insights.append({
                            "type": trend_type,
                            "title": f"{col.replace('_', ' ').title()} Trend",
                            "value": f"{change_pct:+.1f}% change"
                        })
        
        elif numeric_cols and chart_type == 'bar':
            # Top performer analysis for bar charts
            col = numeric_cols[0]
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
            
            if categorical_cols:
                cat_col = categorical_cols[0]
                max_idx = df[col].idxmax()
                top_performer = df.loc[max_idx, cat_col]
                top_value = df.loc[max_idx, col]
                
                insights.append({
                    "type": "success",
                    "title": f"Top {cat_col.replace('_', ' ').title()}",
                    "value": f"{top_performer} ({top_value:,.1f})"
                })
        
        # Statistical insights
        if numeric_cols:
            col = numeric_cols[0]
            col_std = df[col].std()
            col_mean = df[col].mean()
            
            if pd.notna(col_std) and pd.notna(col_mean) and col_mean != 0:
                cv = (col_std / col_mean) * 100
                variability = "High" if cv > 30 else "Moderate" if cv > 15 else "Low"
                
                insights.append({
                    "type": "info",
                    "title": f"{col.replace('_', ' ').title()} Variability",
                    "value": f"{variability} (CV: {cv:.1f}%)"
                })
        
        return insights[:4]  # Limit to 4 insights