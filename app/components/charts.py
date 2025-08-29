"""Streamlit chart components."""

import json
from typing import Dict, Optional

import streamlit as st
import pandas as pd
import altair as alt
import plotly
import plotly.graph_objects as go

from analytics.viz.charts import ChartGenerator


def display_chart(df: pd.DataFrame, chart_type: str = None, title: str = None, 
                 metadata: Dict = None, key: str = None) -> None:
    """
    Display chart in Streamlit based on data and chart type.
    
    Args:
        df: DataFrame to visualize
        chart_type: Type of chart to create
        title: Chart title
        metadata: Query metadata
        key: Unique key for Streamlit components
    """
    if df.empty:
        st.warning("No data to display")
        return
    
    chart_gen = ChartGenerator()
    
    # Auto-select chart type if not provided
    if not chart_type:
        chart_type = chart_gen.auto_select_chart_type(df, metadata)
    
    # Generate chart
    library, chart_data = chart_gen.create_chart(df, chart_type, title, metadata)
    
    # Display based on library
    if library == 'altair':
        chart_json = json.loads(chart_data)
        st.altair_chart(alt.Chart.from_dict(chart_json), use_container_width=True)
    
    elif library == 'plotly':
        fig = plotly.graph_objects.Figure(json.loads(chart_data))
        st.plotly_chart(fig, use_container_width=True, key=key)
    
    elif library == 'table':
        st.markdown(chart_data, unsafe_allow_html=True)
    
    else:
        # Fallback to dataframe
        st.dataframe(df, use_container_width=True)


def chart_type_selector(df: pd.DataFrame, key: str = None) -> str:
    """
    Create chart type selector widget.
    
    Returns:
        Selected chart type
    """
    chart_gen = ChartGenerator()
    suggestions = chart_gen.get_chart_suggestions(df)
    
    # Create options
    options = {}
    for suggestion in suggestions:
        options[suggestion['name']] = suggestion['type']
    
    # Default selection
    auto_type = chart_gen.auto_select_chart_type(df)
    default_name = next(
        (name for name, type_val in options.items() if type_val == auto_type),
        list(options.keys())[0] if options else "Data Table"
    )
    
    selected_name = st.selectbox(
        "Chart Type",
        options=list(options.keys()),
        index=list(options.keys()).index(default_name) if default_name in options else 0,
        key=key,
        help="Choose how to visualize your data"
    )
    
    return options.get(selected_name, 'table')


def download_buttons(df: pd.DataFrame, chart_type: str = None, title: str = "results") -> None:
    """
    Display download buttons for data and charts.
    
    Args:
        df: DataFrame to export
        chart_type: Chart type for image export
        title: Base filename for exports
    """
    col1, col2 = st.columns(2)
    
    with col1:
        # CSV download
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{title}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Chart download (if applicable)
        if chart_type and chart_type != 'table':
            st.button(
                "📊 Download Chart",
                help="Chart download feature coming soon",
                disabled=True
            )


def query_metadata_display(metadata: Dict) -> None:
    """
    Display query execution metadata.
    
    Args:
        metadata: Query execution metadata
    """
    if not metadata:
        return
    
    with st.expander("Query Details", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Rows", metadata.get('row_count', 0))
            
        with col2:
            st.metric("Columns", metadata.get('column_count', 0))
            
        with col3:
            exec_time = metadata.get('execution_time_ms')
            if exec_time:
                exec_time_str = f"{exec_time}ms" if exec_time < 1000 else f"{exec_time/1000:.1f}s"
                st.metric("Execution Time", exec_time_str)
        
        # Additional warehouse-specific details
        warehouse = metadata.get('warehouse', 'Unknown')
        st.text(f"Warehouse: {warehouse}")
        
        if warehouse == 'Snowflake':
            if metadata.get('bytes_scanned'):
                st.text(f"Bytes Scanned: {metadata['bytes_scanned']:,}")
            if metadata.get('query_id'):
                st.text(f"Query ID: {metadata['query_id']}")
        
        elif warehouse == 'BigQuery':
            if metadata.get('bytes_processed'):
                st.text(f"Bytes Processed: {metadata['bytes_processed']:,}")
            if metadata.get('job_id'):
                st.text(f"Job ID: {metadata['job_id']}")
        
        elif warehouse == 'DuckDB':
            if metadata.get('database_path'):
                st.text(f"Database: {metadata['database_path']}")


def data_preview(df: pd.DataFrame, max_rows: int = 100) -> None:
    """
    Display data preview with pagination.
    
    Args:
        df: DataFrame to preview
        max_rows: Maximum rows to display
    """
    if df.empty:
        st.info("No data to preview")
        return
    
    total_rows = len(df)
    
    if total_rows <= max_rows:
        st.dataframe(df, use_container_width=True)
    else:
        # Show pagination info
        st.info(f"Showing first {max_rows} of {total_rows:,} rows")
        st.dataframe(df.head(max_rows), use_container_width=True)


def chart_insights_panel(df: pd.DataFrame, chart_type: str, metadata: Dict = None) -> None:
    """
    Display automatic insights about the chart and data.
    
    Args:
        df: DataFrame being visualized
        chart_type: Type of chart being displayed
        metadata: Query metadata
    """
    if df.empty:
        return
    
    insights = []
    
    # Basic data insights
    total_rows = len(df)
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    insights.append(f"📊 Dataset contains {total_rows:,} rows")
    
    # Chart-specific insights
    if chart_type == 'line' and len(numeric_cols) > 0:
        # Find trends
        for col in numeric_cols[:2]:  # Limit to first 2 numeric columns
            if len(df) > 1:
                first_val = df[col].iloc[0]
                last_val = df[col].iloc[-1]
                if pd.notna(first_val) and pd.notna(last_val) and first_val != 0:
                    change_pct = ((last_val - first_val) / first_val) * 100
                    trend = "📈" if change_pct > 0 else "📉"
                    insights.append(f"{trend} {col.replace('_', ' ').title()}: {change_pct:+.1f}% change")
    
    elif chart_type == 'bar' and len(numeric_cols) > 0:
        # Find top performers
        col = numeric_cols[0]
        categorical_col = df.select_dtypes(include=['object', 'category']).columns
        if len(categorical_col) > 0:
            cat_col = categorical_col[0]
            top_value = df.loc[df[col].idxmax(), cat_col]
            insights.append(f"🏆 Highest {col.replace('_', ' ').title()}: {top_value}")
    
    elif chart_type == 'pie':
        # Find largest segment
        if len(numeric_cols) > 0 and len(df.select_dtypes(include=['object']).columns) > 0:
            val_col = numeric_cols[0]
            cat_col = df.select_dtypes(include=['object']).columns[0]
            total = df[val_col].sum()
            largest_idx = df[val_col].idxmax()
            largest_cat = df.loc[largest_idx, cat_col]
            largest_pct = (df.loc[largest_idx, val_col] / total) * 100
            insights.append(f"🥇 Largest segment: {largest_cat} ({largest_pct:.1f}%)")
    
    # Display insights
    if insights:
        st.info(" • ".join(insights))


def empty_state_message(message: str = None) -> None:
    """
    Display empty state message when no results.
    
    Args:
        message: Custom message to display
    """
    default_message = """
    🤔 **No results found**
    
    Try refining your question or check:
    - Are the table/column names correct?
    - Is the date range appropriate?
    - Are there filters that might be too restrictive?
    """
    
    st.markdown(message or default_message)