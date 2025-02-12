import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any
from sklearn.preprocessing import StandardScaler
from sklearn.covariance import EllipticEnvelope
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import openai
import os
import json
from pathlib import Path
import uuid
from datetime import datetime

def convert_to_serializable(obj):
    """Convert numpy types to Python native types"""
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
        np.int16, np.int32, np.int64, np.uint8,
        np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    elif isinstance(obj, (np.bool_)):
        return bool(obj)
    elif isinstance(obj, (np.void)): 
        return None
    return obj

class DashboardManager:
    def __init__(self):
        """Initialize dashboard manager"""
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.dashboards_file = self.data_dir / "dashboards.json"
        if not self.dashboards_file.exists():
            self.save_dashboards({})
    
    def save_dashboards(self, dashboards: Dict):
        """Save dashboards to file"""
        # Convert numpy types to Python native types
        serializable_dashboards = json.loads(
            json.dumps(dashboards, default=convert_to_serializable)
        )
        with open(self.dashboards_file, 'w') as f:
            json.dump(serializable_dashboards, f)
    
    def load_dashboards(self) -> Dict:
        """Load saved dashboards"""
        try:
            with open(self.dashboards_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def generate_insights(self, df: pd.DataFrame) -> List[Dict]:
        """Generate insights from dataframe"""
        insights = []
        
        # Get main metric column (looking for flu/cases or any numeric column)
        main_col = next((col for col in df.columns if 'flu' in col.lower() or 'cases' in col.lower()), None)
        if not main_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            main_col = next((col for col in numeric_cols if not any(ex in col.lower() for ex in ['id', 'index', 'row'])), None)
        
        if main_col:
            # Calculate basic statistics
            max_value = df[main_col].max()
            avg_value = df[main_col].mean()
            total_value = df[main_col].sum()
            
            # 1. Highest Value
            country_col = next((col for col in df.columns if 'country' in col.lower() or 'territory' in col.lower()), None)
            if country_col:
                max_country = df.loc[df[main_col].idxmax(), country_col]
                insights.append({
                    'title': 'Highest Cases',
                    'value': int(max_value),
                    'description': f'Highest in {max_country}',
                    'type': 'kpi'
                })
            else:
                insights.append({
                    'title': 'Maximum',
                    'value': int(max_value),
                    'description': f'Highest {main_col}',
                    'type': 'kpi'
                })
            
            # 2. Average
            insights.append({
                'title': 'Average',
                'value': round(avg_value, 1),
                'description': f'Average across all records',
                'type': 'kpi'
            })
            
            # 3. Count above average
            above_avg = len(df[df[main_col] > avg_value])
            if country_col:
                insights.append({
                    'title': 'Above Average',
                    'value': above_avg,
                    'description': 'Countries above average',
                    'type': 'kpi'
                })
            else:
                insights.append({
                    'title': 'Above Average',
                    'value': above_avg,
                    'description': 'Records above average',
                    'type': 'kpi'
                })
            
            # 4. Variation Analysis
            std_dev = df[main_col].std()
            cv = (std_dev / avg_value) * 100 if avg_value != 0 else 0
            insights.append({
                'title': 'Variation',
                'value': round(cv, 1),
                'description': 'Coefficient of variation (%)',
                'type': 'kpi'
            })
        
        return insights[:4]  # Return top 4 insights
    
    def generate_visualization_questions(self, df: pd.DataFrame) -> List[Dict]:
        """Generate questions that can be answered with visualizations"""
        questions = []
        
        # Get main metric column
        main_metric = [col for col in df.columns if 'flu' in col.lower() or 'cases' in col.lower()]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_cols = [col for col in numeric_cols if not any(ex in col.lower() for ex in ['id', 'index', 'row'])]
        
        # 1. Geographic Distribution (if country/territory data exists)
        country_col = [col for col in df.columns if 'country' in col.lower() or 'territory' in col.lower()]
        if country_col and main_metric:
            questions.append({
                'question': f'How does {main_metric[0]} vary across different countries?',
                'viz_type': 'map',
                'importance': 1.0
            })
        
        # 2. Top/Bottom Analysis
        if country_col and main_metric:
            questions.append({
                'question': f'Which countries have the highest and lowest {main_metric[0]}?',
                'viz_type': 'bar',
                'importance': 0.9
            })
        
        # 3. Distribution Analysis
        if main_metric:
            questions.append({
                'question': f'What is the distribution pattern of {main_metric[0]}?',
                'viz_type': 'histogram',
                'importance': 0.8
            })
        
        # 4. Correlation Analysis
        if len(numeric_cols) > 2:
            questions.append({
                'question': 'How are different metrics correlated?',
                'viz_type': 'heatmap',
                'importance': 0.7
            })
        
        # Sort by importance and ensure unique viz types
        questions.sort(key=lambda x: x['importance'], reverse=True)
        selected_questions = []
        used_types = set()
        
        for q in questions:
            if q['viz_type'] not in used_types:
                selected_questions.append(q)
                used_types.add(q['viz_type'])
        
        return selected_questions[:4]  # Return top 4 unique visualization types
    
    def create_visualization(self, df: pd.DataFrame, viz_type: str, question: str) -> go.Figure:
        """Create visualization based on type"""
        try:
            # Get main metric column
            main_col = next((col for col in df.columns if 'flu' in col.lower() or 'cases' in col.lower()), None)
            if not main_col:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                main_col = next((col for col in numeric_cols if not any(ex in col.lower() for ex in ['id', 'index', 'row'])), None)
            
            country_col = next((col for col in df.columns if 'country' in col.lower() or 'territory' in col.lower()), None)
            
            if viz_type == 'map' and country_col and main_col:
                # Create choropleth map
                fig = px.choropleth(df, 
                                  locations=country_col,
                                  locationmode="country names",
                                  color=main_col,
                                  title=f"{main_col} by Country",
                                  color_continuous_scale="Viridis")
                fig.update_layout(
                    geo=dict(showframe=False, showcoastlines=True, projection_type='equirectangular'),
                    width=800, height=500
                )
                return fig
            
            elif viz_type == 'bar' and main_col:
                # Create bar chart
                if country_col:
                    df_sorted = df.sort_values(main_col, ascending=False)
                    df_ends = pd.concat([df_sorted.head(10), df_sorted.tail(10)])
                    fig = px.bar(df_ends,
                               x=country_col,
                               y=main_col,
                               title=f"Top and Bottom 10 by {main_col}",
                               color=main_col,
                               color_continuous_scale="Viridis")
                else:
                    df_sorted = df.sort_values(main_col, ascending=False)
                    fig = px.bar(df_sorted.head(20),
                               x=df_sorted.index,
                               y=main_col,
                               title=f"Top 20 by {main_col}")
                return fig
            
            elif viz_type == 'histogram' and main_col:
                # Create histogram of main metric
                fig = px.histogram(df,
                                 x=main_col,
                                 title=f"Distribution of {main_col}",
                                 nbins=30,
                                 color_discrete_sequence=["#1f77b4"])
                fig.update_layout(showlegend=False)
                return fig
            
            elif viz_type == 'heatmap':
                # Create correlation heatmap
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                numeric_cols = [col for col in numeric_cols if not any(ex in col.lower() for ex in ['id', 'index', 'row'])]
                if len(numeric_cols) > 1:
                    corr_matrix = df[numeric_cols].corr()
                    fig = px.imshow(corr_matrix,
                                   title="Correlation Heatmap",
                                   color_continuous_scale="RdBu",
                                   aspect="auto")
                    return fig
            
            # If no specific visualization could be created, create a fallback histogram
            if main_col:
                return px.histogram(df,
                                  x=main_col,
                                  title=f"Distribution of {main_col}",
                                  nbins=30,
                                  color_discrete_sequence=["#1f77b4"])
            
            return None
        except Exception as e:
            print(f"Error creating visualization: {str(e)}")
            return None
    
    def create_dashboard(self, df: pd.DataFrame, title: str = None) -> str:
        """Create a new dashboard and return its ID"""
        dashboard_id = str(uuid.uuid4())
        
        # Clean and preprocess the data
        df = df.copy()  # Create a copy to avoid modifying the original
        
        # Debug prints
        print("\nDataset Information:")
        print("Columns:", df.columns.tolist())
        print("Data Types:\n", df.dtypes)
        print("\nSample Data:\n", df.head(2))
        
        # Find the main numeric column
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # If no numeric columns, try to convert string columns that look like numbers
        if not numeric_cols:
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        # Try to convert strings with commas to numeric
                        df[col] = df[col].str.replace(',', '').astype(float)
                        numeric_cols.append(col)
                    except:
                        continue
        
        # Get the main metric column
        main_col = None
        if numeric_cols:
            # Try to find a relevant column based on common names
            main_col = next((col for col in numeric_cols if any(term in col.lower() 
                for term in ['sales', 'target', 'cases', 'flu', 'value', 'amount'])), numeric_cols[0])
        
        if not main_col:
            st.error("No numeric columns found in the dataset")
            return None
        
        # Generate insights
        insights = [
            {
                'title': f'Total {main_col}',
                'value': int(df[main_col].sum()),
                'description': f'Total {main_col} across all records',
                'type': 'kpi'
            },
            {
                'title': f'Average {main_col}',
                'value': int(df[main_col].mean()),
                'description': f'Average {main_col} per record',
                'type': 'kpi'
            },
            {
                'title': f'Maximum {main_col}',
                'value': int(df[main_col].max()),
                'description': f'Highest {main_col} value',
                'type': 'kpi'
            },
            {
                'title': 'Records',
                'value': len(df),
                'description': 'Total number of records',
                'type': 'kpi'
            }
        ]
        
        # Create visualizations
        visualizations = []
        
        # Check for geographical data
        geo_cols = {
            'country': next((col for col in df.columns if 'country' in col.lower()), None),
            'city': next((col for col in df.columns if 'city' in col.lower()), None),
            'lat': next((col for col in df.columns if any(term in col.lower() for term in ['lat', 'latitude'])), None),
            'lon': next((col for col in df.columns if any(term in col.lower() for term in ['lon', 'long', 'longitude'])), None)
        }
        
        # Check for time-related columns
        time_cols = [col for col in df.columns if any(term in col.lower() 
            for term in ['date', 'year', 'month', 'time', 'period'])]
        
        # Find categorical columns
        categorical_cols = [col for col in df.columns if col != main_col 
                           and df[col].dtype in ['object', 'category']
                           and df[col].nunique() <= 50]  # Limit to columns with reasonable number of categories
        
        viz_count = 0  # Keep track of visualizations added
        
        # 1. Add map visualization if geographical data exists
        if geo_cols['country'] or (geo_cols['lat'] and geo_cols['lon']):
            if geo_cols['country']:
                # Create choropleth map
                fig = px.choropleth(df, 
                                  locations=geo_cols['country'],
                                  locationmode="country names",
                                  color=main_col,
                                  title=f"{main_col} by Country",
                                  color_continuous_scale="Viridis")
                fig.update_layout(
                    geo=dict(showframe=False, showcoastlines=True, projection_type='equirectangular'),
                    width=800, height=500
                )
            else:
                # Create scatter mapbox
                fig = px.scatter_mapbox(df,
                                      lat=geo_cols['lat'],
                                      lon=geo_cols['lon'],
                                      color=main_col,
                                      size=main_col,
                                      title=f"{main_col} by Location",
                                      color_continuous_scale="Viridis")
                fig.update_layout(mapbox_style="carto-positron")
            
            visualizations.append({
                'question': f'How is {main_col} distributed geographically?',
                'viz_type': 'map',
                'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
            })
            viz_count += 1
        
        # 2. Add time series visualization if time-related data exists
        if time_cols and viz_count < 4:
            time_col = time_cols[0]
            try:
                # Convert to datetime if string
                if df[time_col].dtype == 'object':
                    df[time_col] = pd.to_datetime(df[time_col])
                
                # Group by time column and calculate mean of main metric
                time_series = df.groupby(time_col)[main_col].mean().reset_index()
                time_series = time_series.sort_values(time_col)
                
                fig = px.line(time_series,
                             x=time_col,
                             y=main_col,
                             title=f'{main_col} Over Time',
                             markers=True)
                
                visualizations.append({
                    'question': f'How does {main_col} change over time?',
                    'viz_type': 'line',
                    'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
                })
                viz_count += 1
            except:
                pass
        
        # 3. Add bar chart if categorical columns exist and space remains
        if categorical_cols and viz_count < 4:
            primary_cat_col = categorical_cols[0]
            agg_df = df.groupby(primary_cat_col)[main_col].sum().reset_index()
            agg_df = agg_df.sort_values(main_col, ascending=False)
            
            # Only create bar chart if there are multiple categories
            if len(agg_df) > 1:
                fig = px.bar(agg_df.head(10),
                            x=primary_cat_col,
                            y=main_col,
                            title=f'{main_col} by {primary_cat_col} (Top 10)',
                            color=main_col,
                            color_continuous_scale="Viridis")
                fig.update_layout(xaxis_tickangle=-45)
                
                visualizations.append({
                    'question': f'How does {main_col} vary across different {primary_cat_col}?',
                    'viz_type': 'bar',
                    'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
                })
                viz_count += 1
        
        # 4. Add alternative visualizations to fill remaining slots
        remaining_slots = 4 - viz_count
        if remaining_slots > 0:
            # If multiple numeric columns exist, add correlation heatmap
            if len(numeric_cols) > 1:
                corr_matrix = df[numeric_cols].corr()
                fig = px.imshow(corr_matrix,
                              title="Correlation Heatmap",
                              color_continuous_scale="RdBu",
                              aspect="auto")
                visualizations.append({
                    'question': 'How are the numeric variables correlated?',
                    'viz_type': 'heatmap',
                    'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
                })
                viz_count += 1
            
            # If we still have categorical columns and space, add another categorical analysis
            if remaining_slots > 0 and len(categorical_cols) > 1:
                secondary_cat_col = categorical_cols[1]  # Use the second categorical column
                agg_df = df.groupby(secondary_cat_col)[main_col].sum().reset_index()
                agg_df = agg_df.sort_values(main_col, ascending=False)
                
                # Only create pie chart if there are multiple categories
                if len(agg_df) > 1:
                    fig = px.pie(agg_df.head(10),
                               values=main_col,
                               names=secondary_cat_col,
                               title=f'Distribution by {secondary_cat_col} (Top 10)')
                    visualizations.append({
                        'question': f'How is {main_col} distributed across {secondary_cat_col}?',
                        'viz_type': 'pie',
                        'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
                    })
                    viz_count += 1
            
            # If we still have space and time columns, add a box plot by time period
            if remaining_slots > 0 and time_cols:
                try:
                    time_col = time_cols[0]
                    if df[time_col].dtype == 'object':
                        df[time_col] = pd.to_datetime(df[time_col])
                    
                    # Extract period (e.g., month or year) for grouping
                    if df[time_col].dt.year.nunique() > 1:
                        period = df[time_col].dt.year
                        period_name = 'Year'
                    else:
                        period = df[time_col].dt.month
                        period_name = 'Month'
                    
                    # Only create box plot if there are multiple time periods
                    if len(df[period].unique()) > 1:
                        fig = px.box(df,
                                   x=period,
                                   y=main_col,
                                   title=f'{main_col} Distribution by {period_name}')
                        visualizations.append({
                            'question': f'How does the distribution of {main_col} vary over time?',
                            'viz_type': 'box',
                            'figure': json.loads(json.dumps(fig.to_dict(), default=convert_to_serializable))
                        })
                        viz_count += 1
                except:
                    pass
        
        print(f"\nTotal visualizations created: {len(visualizations)}")
        
        # Create dashboard object
        dashboard = {
            'id': dashboard_id,
            'title': title or f'Dashboard {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            'created_at': datetime.now().isoformat(),
            'insights': insights,
            'visualizations': visualizations
        }
        
        # Save dashboard
        dashboards = self.load_dashboards()
        dashboards[dashboard_id] = dashboard
        self.save_dashboards(dashboards)
        
        return dashboard_id
    
    def get_dashboard(self, dashboard_id: str) -> Dict:
        """Get a dashboard by ID"""
        dashboards = self.load_dashboards()
        return dashboards.get(dashboard_id)
    
    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard"""
        dashboards = self.load_dashboards()
        if dashboard_id in dashboards:
            del dashboards[dashboard_id]
            self.save_dashboards(dashboards)
            return True
        return False

def show_dashboard_page(df: pd.DataFrame = None, dashboard_id: str = None):
    """Show dashboard page in Streamlit"""
    # Add back button at the top
    col1, col2 = st.columns([1, 11])
    with col1:
        if st.button("← Back"):
            st.session_state.show_dashboard = False
            st.rerun()
    with col2:
        st.title("📊 Interactive Dashboard")
    
    dashboard_manager = DashboardManager()
    
    if dashboard_id:
        dashboard = dashboard_manager.get_dashboard(dashboard_id)
    elif df is not None:
        # Create new dashboard
        dashboard_id = dashboard_manager.create_dashboard(df)
        dashboard = dashboard_manager.get_dashboard(dashboard_id)
    else:
        st.error("No data or dashboard ID provided")
        return
    
    if not dashboard:
        st.error("Dashboard not found")
        return
    
    # Show refresh button in a small column
    col1, col2, col3 = st.columns([1, 8, 1])
    with col1:
        if df is not None and st.button("🔄", help="Refresh Dashboard"):
            # Store current session state
            current_user = st.session_state.user
            show_dashboard = st.session_state.show_dashboard
            current_dataset = st.session_state.current_dataset
            
            # Create new dashboard
            dashboard_id = dashboard_manager.create_dashboard(df)
            dashboard = dashboard_manager.get_dashboard(dashboard_id)
            
            # Update session state
            st.session_state.current_dashboard_id = dashboard_id
            
            # Restore session state
            st.session_state.user = current_user
            st.session_state.show_dashboard = show_dashboard
            st.session_state.current_dataset = current_dataset
            
            st.rerun()
    
    # Display KPIs in a grid
    st.subheader("📈 Key Performance Indicators")
    kpi_cols = st.columns(4)
    insights = dashboard.get('insights', [])
    
    # Display KPIs
    for i, insight in enumerate(insights[:4]):
        with kpi_cols[i]:
            st.metric(
                label=insight['title'],
                value=insight['value'],
                help=insight['description']
            )
    
    # Display visualizations in a grid
    if dashboard.get('visualizations'):
        st.markdown("---")
        st.subheader("Visualizations")
        
        # Create two rows of two columns each for visualizations
        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)
        
        # Get all columns for visualization placement
        viz_columns = [row1_col1, row1_col2, row2_col1, row2_col2]
        
        # Display visualizations in the grid
        for i, viz in enumerate(dashboard['visualizations'][:4]):
            with viz_columns[i]:
                st.write(f"**{viz['question']}**")
                fig = go.Figure(data=viz['figure'])
                st.plotly_chart(fig, use_container_width=True, key=f"viz_{dashboard_id}_{i}")
    
    # Ask Question Section
    st.markdown("---")
    st.subheader("❓ Ask a Question")
    
    # Create columns for the question section
    question_col1, question_col2 = st.columns([4, 1])
    with question_col1:
        user_question = st.text_input("Ask a question about your data:", placeholder="e.g., Which country has the highest flu cases?")
    with question_col2:
        ask_button = st.button("Ask", type="primary")
    
    if ask_button and user_question and df is not None:
        try:
            # Get numeric columns for analysis
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                main_col = numeric_cols[0]
                
                # Simple question answering logic
                if 'highest' in user_question.lower() or 'most' in user_question.lower():
                    max_val = df[main_col].max()
                    max_row = df.loc[df[main_col].idxmax()]
                    st.success(f"🎯 The highest {main_col} is {round(max_val, 2)}")
                
                elif 'lowest' in user_question.lower() or 'least' in user_question.lower():
                    min_val = df[main_col].min()
                    min_row = df.loc[df[main_col].idxmin()]
                    st.success(f"🎯 The lowest {main_col} is {round(min_val, 2)}")
                
                elif 'average' in user_question.lower() or 'mean' in user_question.lower():
                    avg_val = df[main_col].mean()
                    st.success(f"🎯 The average {main_col} is {round(avg_val, 2)}")
                
                elif 'total' in user_question.lower() or 'sum' in user_question.lower():
                    total = df[main_col].sum()
                    st.success(f"🎯 The total {main_col} is {round(total, 2)}")
                
                else:
                    st.info("I can answer questions about highest/lowest values, averages, and totals. Please try rephrasing your question.")
            else:
                st.warning("No numeric data found to analyze.")
        except Exception as e:
            st.error("Sorry, I couldn't answer that question. Please try asking something else.") 