"""Streamlit application for the BI Assistant."""

import os
import traceback
from typing import Dict, Optional

import streamlit as st
import pandas as pd

# Import our modules
from app.config import Config, WarehouseType
from app.components.charts import (
    display_chart, chart_type_selector, download_buttons,
    query_metadata_display, data_preview, chart_insights_panel,
    empty_state_message
)
from analytics.nl2sql.agent import create_agent
from analytics.runners import create_runner, get_available_warehouses
from analytics.insights.suggest import InsightGenerator

# Page configuration
st.set_page_config(
    page_title="BI Assistant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
        color: #1f77b4;
    }
    .query-box {
        border: 2px solid #e6e9ef;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        background-color: #f8f9fa;
    }
    .insight-card {
        border: 1px solid #dee2e6;
        border-radius: 6px;
        padding: 0.75rem;
        margin: 0.5rem 0;
        background-color: white;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_results' not in st.session_state:
    st.session_state.current_results = None
if 'warehouse_runner' not in st.session_state:
    st.session_state.warehouse_runner = None
if 'agent' not in st.session_state:
    st.session_state.agent = None


def initialize_connections():
    """Initialize database and agent connections."""
    try:
        if st.session_state.warehouse_runner is None:
            st.session_state.warehouse_runner = create_runner()
            
        if st.session_state.agent is None:
            st.session_state.agent = create_agent(st.session_state.warehouse_runner)
            
        return True
    except Exception as e:
        st.error(f"Failed to initialize connections: {str(e)}")
        return False


def sidebar():
    """Render sidebar with configuration and controls."""
    st.sidebar.title("⚙️ Configuration")
    
    # Warehouse selection
    available_warehouses = get_available_warehouses()
    current_warehouse = Config.WAREHOUSE
    
    st.sidebar.markdown("### Data Warehouse")
    
    warehouse_options = {}
    for wh in available_warehouses:
        if wh == WarehouseType.DUCKDB:
            warehouse_options["🦆 DuckDB (Local)"] = wh
        elif wh == WarehouseType.SNOWFLAKE:
            warehouse_options["❄️ Snowflake"] = wh
        elif wh == WarehouseType.BIGQUERY:
            warehouse_options["🔍 BigQuery"] = wh
    
    current_display = next(
        (name for name, wh in warehouse_options.items() if wh == current_warehouse),
        list(warehouse_options.keys())[0]
    )
    
    selected_warehouse = st.sidebar.selectbox(
        "Active Warehouse",
        options=list(warehouse_options.keys()),
        index=list(warehouse_options.keys()).index(current_display),
        disabled=True,  # For now, disable changing warehouse in UI
        help="Warehouse is configured via environment variables"
    )
    
    # Connection status
    if st.session_state.warehouse_runner:
        try:
            if st.session_state.warehouse_runner.test_connection():
                st.sidebar.success("✅ Connected")
            else:
                st.sidebar.error("❌ Connection failed")
        except:
            st.sidebar.warning("⚠️ Connection status unknown")
    
    # Query examples
    st.sidebar.markdown("### 📝 Example Questions")
    
    example_queries = [
        "Show me attrition trends for Q2 in North America vs EMEA",
        "What's the current headcount by department?",
        "Show attrition rate by tenure groups for the last 12 months",
        "Which regions have the highest hiring activity this quarter?",
        "What's the average salary by department and gender?",
        "Show me monthly attrition trends for the past year"
    ]
    
    for i, example in enumerate(example_queries):
        if st.sidebar.button(f"💡 {example[:40]}...", key=f"example_{i}"):
            st.session_state.example_query = example
    
    # Query history
    if st.session_state.query_history:
        st.sidebar.markdown("### 📚 Recent Queries")
        for i, query in enumerate(reversed(st.session_state.query_history[-5:])):
            if st.sidebar.button(f"🔄 {query[:30]}...", key=f"history_{i}"):
                st.session_state.example_query = query
    
    # Debug info
    if Config.DEBUG:
        st.sidebar.markdown("### 🔧 Debug Info")
        st.sidebar.json({
            "Warehouse": Config.WAREHOUSE.value,
            "LLM Provider": Config.LLM_PROVIDER.value,
            "Vector Backend": Config.VECTOR_BACKEND.value
        })


def main_interface():
    """Render main application interface."""
    # Header
    st.markdown('<div class="main-header">📊 BI Assistant</div>', unsafe_allow_html=True)
    st.markdown("Ask questions about your data in natural language and get instant insights.")
    
    # Query input
    query_placeholder = "e.g., Show me attrition trends by department for the last quarter"
    
    # Use example query if selected
    default_query = st.session_state.get('example_query', '')
    if default_query and 'example_query' in st.session_state:
        del st.session_state.example_query
    
    user_query = st.text_area(
        "What would you like to know about your data?",
        value=default_query,
        placeholder=query_placeholder,
        height=100,
        help="Ask questions in plain English about employees, attrition, hiring, or any HR metrics"
    )
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        run_query = st.button("🔍 Analyze", type="primary", disabled=not user_query.strip())
    
    with col2:
        if st.button("🔄 Clear Results"):
            st.session_state.current_results = None
            st.rerun()
    
    with col3:
        if st.button("🏗️ Rebuild Index"):
            with st.spinner("Rebuilding schema index..."):
                try:
                    if st.session_state.agent:
                        st.session_state.agent.rebuild_schema_index()
                        st.success("Schema index rebuilt successfully!")
                except Exception as e:
                    st.error(f"Failed to rebuild index: {str(e)}")
    
    # Process query
    if run_query and user_query.strip():
        process_query(user_query.strip())
    
    # Display results
    if st.session_state.current_results:
        display_results()


def process_query(query: str):
    """Process a natural language query."""
    with st.spinner("Analyzing your question..."):
        try:
            # Translate to SQL
            success, sql, error = st.session_state.agent.translate_to_sql(query)
            
            if not success:
                st.error(f"❌ Could not generate SQL: {error}")
                return
            
            # Show generated SQL
            with st.expander("📋 Generated SQL", expanded=False):
                st.code(sql, language="sql")
            
            # Execute query
            with st.spinner("Executing query..."):
                df, metadata = st.session_state.warehouse_runner.execute_query(sql)
            
            # Generate insights
            insight_gen = InsightGenerator()
            narrative = insight_gen.generate_narrative(df, query, sql, metadata)
            follow_ups = insight_gen.generate_follow_up_questions(df, query, sql)
            
            # Store results
            st.session_state.current_results = {
                'query': query,
                'sql': sql,
                'data': df,
                'metadata': metadata,
                'narrative': narrative,
                'follow_ups': follow_ups
            }
            
            # Add to history
            if query not in st.session_state.query_history:
                st.session_state.query_history.append(query)
            
            st.success(f"✅ Found {len(df):,} results")
            
        except Exception as e:
            st.error(f"❌ Query execution failed: {str(e)}")
            if Config.DEBUG:
                st.error(traceback.format_exc())


def display_results():
    """Display query results with visualizations and insights."""
    results = st.session_state.current_results
    df = results['data']
    metadata = results['metadata']
    
    if df.empty:
        empty_state_message()
        return
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Visualization", "📋 Data", "💡 Insights", "🔍 Details"])
    
    with tab1:
        # Chart type selector and visualization
        chart_type = chart_type_selector(df, key="main_chart_selector")
        
        # Display chart
        display_chart(
            df, 
            chart_type=chart_type, 
            title=results['query'],
            metadata=metadata,
            key="main_chart"
        )
        
        # Download buttons
        download_buttons(df, chart_type, title="query_results")
        
        # Quick insights panel
        chart_insights_panel(df, chart_type, metadata)
    
    with tab2:
        # Data preview
        st.markdown("### 📋 Query Results")
        data_preview(df, max_rows=100)
        
        # Basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            st.markdown("### 📈 Summary Statistics")
            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    with tab3:
        # Narrative insights
        if results.get('narrative'):
            st.markdown("### 📖 What the data tells us")
            st.markdown(f"💬 {results['narrative']}")
        
        # Follow-up questions
        if results.get('follow_ups'):
            st.markdown("### 🤔 Explore further")
            st.markdown("*Click on any question below to run it:*")
            
            for i, question in enumerate(results['follow_ups']):
                if st.button(f"❓ {question}", key=f"followup_{i}"):
                    st.session_state.example_query = question
                    st.rerun()
        
        # Key insights
        try:
            insight_gen = InsightGenerator()
            key_insights = insight_gen.generate_key_insights(df, chart_type)
            
            if key_insights:
                st.markdown("### 🎯 Key Insights")
                for insight in key_insights:
                    icon = "📊" if insight['type'] == 'info' else "📈" if insight['type'] == 'success' else "⚠️"
                    st.markdown(f"{icon} **{insight['title']}**: {insight['value']}")
        except Exception as e:
            if Config.DEBUG:
                st.error(f"Error generating insights: {e}")
    
    with tab4:
        # Query metadata
        query_metadata_display(metadata)
        
        # SQL query details
        st.markdown("### 🔍 Query Details")
        st.markdown(f"**Original Question:** {results['query']}")
        
        with st.expander("Generated SQL Query", expanded=False):
            st.code(results['sql'], language="sql")
        
        # Execution plan (if available)
        if hasattr(st.session_state.warehouse_runner, 'get_query_plan'):
            with st.expander("Query Execution Plan", expanded=False):
                try:
                    plan = st.session_state.warehouse_runner.get_query_plan(results['sql'])
                    st.text(plan)
                except Exception as e:
                    st.text(f"Could not retrieve execution plan: {e}")


def main():
    """Main application entry point."""
    # Sidebar
    sidebar()
    
    # Initialize connections
    if not initialize_connections():
        st.error("Failed to initialize. Please check your configuration.")
        st.stop()
    
    # Main interface
    main_interface()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "💡 **Tip:** Be specific in your questions for better results. "
        "Try mentioning time periods, departments, or specific metrics."
    )


if __name__ == "__main__":
    main()