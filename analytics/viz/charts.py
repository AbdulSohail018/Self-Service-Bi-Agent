"""Chart generation utilities for data visualization."""

from typing import Dict, List, Optional, Tuple

import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class ChartGenerator:
    """Generate charts from query results with automatic type detection."""

    def __init__(self):
        """Initialize chart generator."""
        # Set Altair theme
        alt.themes.enable('fivethirtyeight')

    def auto_select_chart_type(self, df: pd.DataFrame, metadata: Dict = None) -> str:
        """
        Automatically select the best chart type based on data characteristics.
        
        Returns:
            Chart type string: 'line', 'bar', 'pie', 'scatter', 'heatmap', 'table', 'kpi'
        """
        if df.empty:
            return 'table'
        
        # Check for single value (KPI)
        if len(df) == 1 and len(df.columns) <= 3:
            return 'kpi'
        
        # Analyze column types
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Time series data
        if len(date_cols) >= 1 and len(numeric_cols) >= 1:
            return 'line'
        
        # Single categorical + numeric = bar chart
        if len(categorical_cols) == 1 and len(numeric_cols) >= 1:
            # Pie chart for percentages or small categories
            if len(df) <= 8 and any('pct' in col.lower() or 'percent' in col.lower() 
                                   or 'rate' in col.lower() for col in numeric_cols):
                return 'pie'
            return 'bar'
        
        # Two numerics = scatter plot
        if len(numeric_cols) >= 2 and len(categorical_cols) <= 1:
            return 'scatter'
        
        # Multiple categories and numerics = heatmap
        if len(categorical_cols) >= 2 and len(numeric_cols) >= 1:
            return 'heatmap'
        
        # Default to table for complex data
        if len(df.columns) > 6 or len(df) > 50:
            return 'table'
        
        return 'bar'  # Default fallback

    def create_chart(self, df: pd.DataFrame, chart_type: str = None, title: str = None, 
                    metadata: Dict = None, **kwargs) -> Tuple[str, Optional[str]]:
        """
        Create a chart based on data and type.
        
        Returns:
            Tuple of (chart_library, chart_object)
            - chart_library: 'altair', 'plotly', or 'table'
            - chart_object: JSON string for Altair/Plotly or HTML for table
        """
        if df.empty:
            return 'table', df.to_html(classes='table table-striped')
        
        # Auto-select chart type if not provided
        if not chart_type:
            chart_type = self.auto_select_chart_type(df, metadata)
        
        # Generate appropriate chart
        if chart_type == 'kpi':
            return self._create_kpi_cards(df, title)
        elif chart_type == 'line':
            return self._create_line_chart(df, title, **kwargs)
        elif chart_type == 'bar':
            return self._create_bar_chart(df, title, **kwargs)
        elif chart_type == 'pie':
            return self._create_pie_chart(df, title, **kwargs)
        elif chart_type == 'scatter':
            return self._create_scatter_plot(df, title, **kwargs)
        elif chart_type == 'heatmap':
            return self._create_heatmap(df, title, **kwargs)
        else:  # table
            return self._create_table(df, title)

    def _create_kpi_cards(self, df: pd.DataFrame, title: str = None) -> Tuple[str, str]:
        """Create KPI cards for single-row results."""
        # Use Plotly for KPI cards
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if len(numeric_cols) == 1:
            # Single KPI
            value = df.iloc[0][numeric_cols[0]]
            
            fig = go.Figure(go.Indicator(
                mode="number",
                value=value,
                title={"text": title or numeric_cols[0].replace('_', ' ').title()},
                number={'font': {'size': 48}}
            ))
            
            fig.update_layout(
                height=300,
                margin=dict(t=50, b=50, l=50, r=50)
            )
            
        else:
            # Multiple KPIs
            cols = min(3, len(numeric_cols))
            rows = (len(numeric_cols) + cols - 1) // cols
            
            fig = make_subplots(
                rows=rows, cols=cols,
                subplot_titles=[col.replace('_', ' ').title() for col in numeric_cols],
                specs=[[{"type": "indicator"}] * cols for _ in range(rows)]
            )
            
            for i, col in enumerate(numeric_cols):
                row = i // cols + 1
                col_idx = i % cols + 1
                
                fig.add_trace(
                    go.Indicator(
                        mode="number",
                        value=df.iloc[0][col],
                        number={'font': {'size': 24}}
                    ),
                    row=row, col=col_idx
                )
            
            fig.update_layout(height=200 * rows, title_text=title)
        
        return 'plotly', fig.to_json()

    def _create_line_chart(self, df: pd.DataFrame, title: str = None, **kwargs) -> Tuple[str, str]:
        """Create line chart for time series data."""
        # Find date and numeric columns
        date_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if not date_cols:
            # Try to convert first column to date
            first_col = df.columns[0]
            try:
                df[first_col] = pd.to_datetime(df[first_col])
                date_cols = [first_col]
            except:
                pass
        
        if not date_cols or not numeric_cols:
            return self._create_table(df, title)
        
        x_col = date_cols[0]
        y_col = numeric_cols[0]
        
        # Use Altair for line charts
        chart = alt.Chart(df).mark_line(
            point=True,
            strokeWidth=3
        ).encode(
            x=alt.X(f'{x_col}:T', title=x_col.replace('_', ' ').title()),
            y=alt.Y(f'{y_col}:Q', title=y_col.replace('_', ' ').title()),
            tooltip=[f'{x_col}:T', f'{y_col}:Q']
        ).properties(
            width=600,
            height=400,
            title=title or f"{y_col.replace('_', ' ').title()} Over Time"
        ).interactive()
        
        # Add additional lines if multiple numeric columns
        if len(numeric_cols) > 1:
            # Melt dataframe for multiple lines
            id_vars = [x_col]
            value_vars = numeric_cols
            melted_df = df.melt(id_vars=id_vars, value_vars=value_vars)
            
            chart = alt.Chart(melted_df).mark_line(
                point=True,
                strokeWidth=2
            ).encode(
                x=alt.X(f'{x_col}:T', title=x_col.replace('_', ' ').title()),
                y=alt.Y('value:Q', title='Value'),
                color=alt.Color('variable:N', title='Metric'),
                tooltip=[f'{x_col}:T', 'variable:N', 'value:Q']
            ).properties(
                width=600,
                height=400,
                title=title or "Trends Over Time"
            ).interactive()
        
        return 'altair', chart.to_json()

    def _create_bar_chart(self, df: pd.DataFrame, title: str = None, **kwargs) -> Tuple[str, str]:
        """Create bar chart for categorical data."""
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if not categorical_cols or not numeric_cols:
            return self._create_table(df, title)
        
        x_col = categorical_cols[0]
        y_col = numeric_cols[0]
        
        # Sort by value for better visualization
        df_sorted = df.sort_values(y_col, ascending=False)
        
        chart = alt.Chart(df_sorted).mark_bar().encode(
            x=alt.X(f'{y_col}:Q', title=y_col.replace('_', ' ').title()),
            y=alt.Y(f'{x_col}:N', sort='-x', title=x_col.replace('_', ' ').title()),
            tooltip=[f'{x_col}:N', f'{y_col}:Q']
        ).properties(
            width=600,
            height=max(300, len(df) * 25),
            title=title or f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}"
        ).interactive()
        
        return 'altair', chart.to_json()

    def _create_pie_chart(self, df: pd.DataFrame, title: str = None, **kwargs) -> Tuple[str, str]:
        """Create pie chart for categorical proportions."""
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if not categorical_cols or not numeric_cols:
            return self._create_table(df, title)
        
        labels_col = categorical_cols[0]
        values_col = numeric_cols[0]
        
        # Use Plotly for pie charts
        fig = px.pie(
            df,
            values=values_col,
            names=labels_col,
            title=title or f"{values_col.replace('_', ' ').title()} Distribution"
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=500)
        
        return 'plotly', fig.to_json()

    def _create_scatter_plot(self, df: pd.DataFrame, title: str = None, **kwargs) -> Tuple[str, str]:
        """Create scatter plot for numeric relationships."""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        if len(numeric_cols) < 2:
            return self._create_table(df, title)
        
        x_col = numeric_cols[0]
        y_col = numeric_cols[1]
        color_col = categorical_cols[0] if categorical_cols else None
        
        encoding = {
            'x': alt.X(f'{x_col}:Q', title=x_col.replace('_', ' ').title()),
            'y': alt.Y(f'{y_col}:Q', title=y_col.replace('_', ' ').title()),
            'tooltip': [f'{x_col}:Q', f'{y_col}:Q']
        }
        
        if color_col:
            encoding['color'] = alt.Color(f'{color_col}:N', title=color_col.replace('_', ' ').title())
            encoding['tooltip'].append(f'{color_col}:N')
        
        chart = alt.Chart(df).mark_circle(size=100).encode(**encoding).properties(
            width=600,
            height=400,
            title=title or f"{y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}"
        ).interactive()
        
        return 'altair', chart.to_json()

    def _create_heatmap(self, df: pd.DataFrame, title: str = None, **kwargs) -> Tuple[str, str]:
        """Create heatmap for categorical relationships."""
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if len(categorical_cols) < 2 or not numeric_cols:
            return self._create_table(df, title)
        
        x_col = categorical_cols[0]
        y_col = categorical_cols[1]
        value_col = numeric_cols[0]
        
        chart = alt.Chart(df).mark_rect().encode(
            x=alt.X(f'{x_col}:O', title=x_col.replace('_', ' ').title()),
            y=alt.Y(f'{y_col}:O', title=y_col.replace('_', ' ').title()),
            color=alt.Color(f'{value_col}:Q', 
                          scale=alt.Scale(scheme='blues'),
                          title=value_col.replace('_', ' ').title()),
            tooltip=[f'{x_col}:O', f'{y_col}:O', f'{value_col}:Q']
        ).properties(
            width=600,
            height=400,
            title=title or "Heatmap"
        )
        
        return 'altair', chart.to_json()

    def _create_table(self, df: pd.DataFrame, title: str = None) -> Tuple[str, str]:
        """Create formatted table."""
        # Format numeric columns
        formatted_df = df.copy()
        for col in df.select_dtypes(include=['number']).columns:
            if df[col].dtype == 'float64':
                formatted_df[col] = df[col].round(2)
        
        # Create HTML table
        html_table = formatted_df.to_html(
            classes='table table-striped table-hover',
            table_id='results-table',
            escape=False,
            index=False
        )
        
        if title:
            html_table = f"<h4>{title}</h4>\n{html_table}"
        
        return 'table', html_table

    def get_chart_suggestions(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """Get suggestions for different chart types based on data."""
        suggestions = []
        
        # Analyze data characteristics
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64', 'datetime']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Always suggest table
        suggestions.append({
            "type": "table",
            "name": "Data Table",
            "description": "View raw data in tabular format"
        })
        
        # Single value - KPI
        if len(df) == 1 and len(numeric_cols) >= 1:
            suggestions.append({
                "type": "kpi",
                "name": "KPI Cards",
                "description": "Display key metrics as cards"
            })
        
        # Time series
        if date_cols and numeric_cols:
            suggestions.append({
                "type": "line",
                "name": "Line Chart",
                "description": "Show trends over time"
            })
        
        # Categorical + numeric
        if categorical_cols and numeric_cols:
            suggestions.append({
                "type": "bar",
                "name": "Bar Chart",
                "description": "Compare values across categories"
            })
            
            if len(df) <= 8:
                suggestions.append({
                    "type": "pie",
                    "name": "Pie Chart",
                    "description": "Show proportional breakdown"
                })
        
        # Two numerics
        if len(numeric_cols) >= 2:
            suggestions.append({
                "type": "scatter",
                "name": "Scatter Plot",
                "description": "Explore relationships between metrics"
            })
        
        # Multiple categories
        if len(categorical_cols) >= 2 and numeric_cols:
            suggestions.append({
                "type": "heatmap",
                "name": "Heatmap",
                "description": "Visualize patterns across dimensions"
            })
        
        return suggestions