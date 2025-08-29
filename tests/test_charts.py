"""Tests for chart generation and visualization components."""

import pytest
import pandas as pd
import json
from datetime import datetime, timedelta

from analytics.viz.charts import ChartGenerator


class TestChartGenerator:
    """Test cases for chart generation functionality."""

    @pytest.fixture
    def chart_gen(self):
        """Create chart generator instance."""
        return ChartGenerator()

    @pytest.fixture
    def sample_timeseries_data(self):
        """Create sample time series data."""
        dates = pd.date_range('2024-01-01', periods=12, freq='M')
        return pd.DataFrame({
            'month': dates,
            'attrition_count': [15, 18, 12, 20, 25, 30, 22, 28, 35, 40, 32, 28],
            'headcount': [1000, 995, 990, 985, 975, 960, 950, 940, 925, 910, 900, 890]
        })

    @pytest.fixture
    def sample_categorical_data(self):
        """Create sample categorical data."""
        return pd.DataFrame({
            'department': ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'],
            'headcount': [150, 120, 80, 45, 35],
            'avg_salary': [95000, 75000, 68000, 62000, 70000]
        })

    @pytest.fixture
    def sample_kpi_data(self):
        """Create sample KPI data."""
        return pd.DataFrame({
            'total_employees': [1250],
            'attrition_rate': [12.5],
            'avg_tenure_years': [3.2]
        })

    def test_auto_chart_type_selection_timeseries(self, chart_gen, sample_timeseries_data):
        """Test automatic chart type selection for time series data."""
        chart_type = chart_gen.auto_select_chart_type(sample_timeseries_data)
        assert chart_type == 'line'

    def test_auto_chart_type_selection_categorical(self, chart_gen, sample_categorical_data):
        """Test automatic chart type selection for categorical data."""
        chart_type = chart_gen.auto_select_chart_type(sample_categorical_data)
        assert chart_type == 'bar'

    def test_auto_chart_type_selection_kpi(self, chart_gen, sample_kpi_data):
        """Test automatic chart type selection for KPI data."""
        chart_type = chart_gen.auto_select_chart_type(sample_kpi_data)
        assert chart_type == 'kpi'

    def test_auto_chart_type_selection_empty_data(self, chart_gen):
        """Test chart type selection for empty data."""
        empty_df = pd.DataFrame()
        chart_type = chart_gen.auto_select_chart_type(empty_df)
        assert chart_type == 'table'

    def test_line_chart_creation(self, chart_gen, sample_timeseries_data):
        """Test line chart creation for time series data."""
        library, chart_data = chart_gen.create_chart(
            sample_timeseries_data, 
            chart_type='line',
            title='Attrition Trends'
        )
        
        assert library == 'altair'
        assert isinstance(chart_data, str)
        
        # Parse and validate Altair JSON
        chart_json = json.loads(chart_data)
        assert 'mark' in chart_json
        assert 'encoding' in chart_json

    def test_bar_chart_creation(self, chart_gen, sample_categorical_data):
        """Test bar chart creation for categorical data."""
        library, chart_data = chart_gen.create_chart(
            sample_categorical_data,
            chart_type='bar',
            title='Headcount by Department'
        )
        
        assert library == 'altair'
        assert isinstance(chart_data, str)
        
        chart_json = json.loads(chart_data)
        assert 'mark' in chart_json
        assert chart_json['mark'] == 'bar'

    def test_pie_chart_creation(self, chart_gen, sample_categorical_data):
        """Test pie chart creation for categorical data."""
        library, chart_data = chart_gen.create_chart(
            sample_categorical_data,
            chart_type='pie',
            title='Department Distribution'
        )
        
        assert library == 'plotly'
        assert isinstance(chart_data, str)
        
        # Validate that it's valid JSON
        chart_json = json.loads(chart_data)
        assert 'data' in chart_json

    def test_kpi_cards_creation(self, chart_gen, sample_kpi_data):
        """Test KPI cards creation for single-row data."""
        library, chart_data = chart_gen.create_chart(
            sample_kpi_data,
            chart_type='kpi',
            title='Key Metrics'
        )
        
        assert library == 'plotly'
        assert isinstance(chart_data, str)
        
        chart_json = json.loads(chart_data)
        assert 'data' in chart_json

    def test_scatter_plot_creation(self, chart_gen):
        """Test scatter plot creation for numeric relationships."""
        scatter_data = pd.DataFrame({
            'salary': [50000, 60000, 70000, 80000, 90000],
            'tenure_years': [1, 2, 3, 5, 7],
            'department': ['HR', 'IT', 'Sales', 'Engineering', 'Finance']
        })
        
        library, chart_data = chart_gen.create_chart(
            scatter_data,
            chart_type='scatter',
            title='Salary vs Tenure'
        )
        
        assert library == 'altair'
        chart_json = json.loads(chart_data)
        assert 'mark' in chart_json

    def test_table_creation(self, chart_gen, sample_categorical_data):
        """Test table creation for data display."""
        library, chart_data = chart_gen.create_chart(
            sample_categorical_data,
            chart_type='table',
            title='Department Data'
        )
        
        assert library == 'table'
        assert isinstance(chart_data, str)
        assert '<table' in chart_data
        assert 'Engineering' in chart_data

    def test_empty_data_handling(self, chart_gen):
        """Test handling of empty datasets."""
        empty_df = pd.DataFrame()
        
        library, chart_data = chart_gen.create_chart(empty_df)
        
        assert library == 'table'
        assert isinstance(chart_data, str)

    def test_chart_suggestions_generation(self, chart_gen, sample_timeseries_data):
        """Test chart type suggestions based on data characteristics."""
        suggestions = chart_gen.get_chart_suggestions(sample_timeseries_data)
        
        assert len(suggestions) > 0
        assert all('type' in suggestion for suggestion in suggestions)
        assert all('name' in suggestion for suggestion in suggestions)
        assert all('description' in suggestion for suggestion in suggestions)
        
        # Should suggest line chart for time series
        suggestion_types = [s['type'] for s in suggestions]
        assert 'line' in suggestion_types
        assert 'table' in suggestion_types

    def test_chart_suggestions_categorical(self, chart_gen, sample_categorical_data):
        """Test chart suggestions for categorical data."""
        suggestions = chart_gen.get_chart_suggestions(sample_categorical_data)
        
        suggestion_types = [s['type'] for s in suggestions]
        assert 'bar' in suggestion_types
        assert 'pie' in suggestion_types
        assert 'scatter' in suggestion_types  # Multiple numeric columns

    def test_chart_suggestions_kpi(self, chart_gen, sample_kpi_data):
        """Test chart suggestions for KPI data."""
        suggestions = chart_gen.get_chart_suggestions(sample_kpi_data)
        
        suggestion_types = [s['type'] for s in suggestions]
        assert 'kpi' in suggestion_types

    def test_heatmap_creation(self, chart_gen):
        """Test heatmap creation for categorical relationships."""
        heatmap_data = pd.DataFrame({
            'department': ['Engineering', 'Engineering', 'Sales', 'Sales', 'Marketing', 'Marketing'],
            'region': ['North', 'South', 'North', 'South', 'North', 'South'],
            'avg_salary': [95000, 92000, 75000, 73000, 68000, 66000]
        })
        
        library, chart_data = chart_gen.create_chart(
            heatmap_data,
            chart_type='heatmap',
            title='Salary Heatmap'
        )
        
        assert library == 'altair'
        chart_json = json.loads(chart_data)
        assert 'mark' in chart_json

    def test_large_dataset_chart_selection(self, chart_gen):
        """Test chart selection for large datasets."""
        # Create large dataset
        large_data = pd.DataFrame({
            'id': range(1000),
            'value': range(1000),
            'category': ['A', 'B', 'C'] * 334  # Repeating pattern
        })
        
        chart_type = chart_gen.auto_select_chart_type(large_data)
        assert chart_type == 'table'  # Should default to table for large datasets

    def test_percentage_data_pie_chart(self, chart_gen):
        """Test that percentage data suggests pie charts."""
        percentage_data = pd.DataFrame({
            'reason': ['Career Growth', 'Compensation', 'Work-Life Balance', 'Management'],
            'attrition_pct': [35.2, 28.5, 20.1, 16.2]
        })
        
        chart_type = chart_gen.auto_select_chart_type(percentage_data)
        assert chart_type == 'pie'

    def test_multiple_numeric_columns_line_chart(self, chart_gen):
        """Test line chart with multiple numeric columns."""
        multi_metric_data = pd.DataFrame({
            'month': pd.date_range('2024-01-01', periods=6, freq='M'),
            'hires': [25, 30, 22, 35, 28, 32],
            'terminations': [18, 22, 15, 28, 20, 25],
            'net_change': [7, 8, 7, 7, 8, 7]
        })
        
        library, chart_data = chart_gen.create_chart(
            multi_metric_data,
            chart_type='line',
            title='HR Metrics Over Time'
        )
        
        assert library == 'altair'
        chart_json = json.loads(chart_data)
        # Should handle multiple lines
        assert 'encoding' in chart_json

    def test_chart_title_handling(self, chart_gen, sample_categorical_data):
        """Test that chart titles are properly applied."""
        custom_title = "Custom Chart Title"
        
        library, chart_data = chart_gen.create_chart(
            sample_categorical_data,
            chart_type='bar',
            title=custom_title
        )
        
        if library == 'altair':
            chart_json = json.loads(chart_data)
            assert 'title' in chart_json
        elif library == 'table':
            assert custom_title in chart_data

    def test_data_type_detection(self, chart_gen):
        """Test proper detection of different data types."""
        mixed_data = pd.DataFrame({
            'date_col': pd.date_range('2024-01-01', periods=5),
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'string_col': ['A', 'B', 'C', 'D', 'E'],
            'bool_col': [True, False, True, False, True]
        })
        
        chart_type = chart_gen.auto_select_chart_type(mixed_data)
        # Should detect date column and suggest time series
        assert chart_type == 'line'

    def test_error_handling_invalid_chart_type(self, chart_gen, sample_categorical_data):
        """Test error handling for invalid chart types."""
        library, chart_data = chart_gen.create_chart(
            sample_categorical_data,
            chart_type='invalid_type'
        )
        
        # Should fallback to table
        assert library == 'table'