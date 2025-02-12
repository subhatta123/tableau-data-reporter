import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any
import streamlit as st
from sklearn.preprocessing import StandardScaler
from sklearn.covariance import EllipticEnvelope
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import openai
import os

class DataAnalyzer:
    def __init__(self):
        """Initialize the analyzer with OpenAI"""
        try:
            openai.api_key = os.getenv('OPENAI_API_KEY')  # Set API key directly
            self.llm_available = True
        except Exception as e:
            st.warning(f"LLM not available: {str(e)}")
            self.llm_available = False

    def generate_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive summary statistics"""
        summary = {
            'basic_stats': df.describe(),
            'missing_values': df.isnull().sum(),
            'data_types': df.dtypes,
            'unique_counts': df.nunique(),
        }
        
        # Add correlation matrix for numerical columns
        num_cols = df.select_dtypes(include=[np.number]).columns
        if len(num_cols) > 0:
            summary['correlation'] = df[num_cols].corr()
        
        return summary

    def detect_anomalies(self, df: pd.DataFrame) -> Dict:
        """Detect anomalies in numerical columns"""
        anomalies = {}
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numerical_cols:
            # Skip columns with too many missing values
            if df[col].isnull().sum() / len(df) > 0.5:
                continue
                
            data = df[col].dropna().values.reshape(-1, 1)
            if len(data) < 10:  # Skip if too few samples
                continue
                
            try:
                # Use robust covariance estimation
                detector = EllipticEnvelope(contamination=0.1, random_state=42)
                detector.fit(data)
                labels = detector.predict(data)
                
                # Get anomaly indices
                anomaly_indices = np.where(labels == -1)[0]
                if len(anomaly_indices) > 0:
                    anomalies[col] = {
                        'indices': anomaly_indices,
                        'values': data[anomaly_indices].flatten(),
                        'count': len(anomaly_indices)
                    }
            except:
                continue
        
        return anomalies

    def create_visualizations(self, df: pd.DataFrame) -> Dict:
        """Create a set of relevant visualizations"""
        plots = {}
        
        # Distribution plots for numerical columns
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            fig = px.histogram(df, x=col, title=f"Distribution of {col}")
            plots[f'dist_{col}'] = fig
            
            # Box plot
            fig = px.box(df, y=col, title=f"Box Plot of {col}")
            plots[f'box_{col}'] = fig
        
        # Correlation heatmap
        if len(num_cols) > 1:
            corr_matrix = df[num_cols].corr()
            fig = px.imshow(
                corr_matrix,
                title="Correlation Matrix",
                color_continuous_scale="RdBu"
            )
            plots['correlation'] = fig
        
        return plots

    def generate_insights(self, df: pd.DataFrame) -> List[str]:
        """Generate basic insights about the data"""
        insights = []
        
        # Sample size
        insights.append(f"Dataset contains {len(df)} rows and {len(df.columns)} columns")
        
        # Missing values
        missing = df.isnull().sum()
        if missing.any():
            insights.append("Missing values found in columns: " + 
                          ", ".join(f"{col} ({val} missing)" 
                          for col, val in missing[missing > 0].items()))
        
        # Numerical columns analysis
        num_cols = df.select_dtypes(include=[np.number]).columns
        for col in num_cols:
            stats = df[col].describe()
            insights.append(f"\nInsights for {col}:")
            insights.append(f"- Range: {stats['min']:.2f} to {stats['max']:.2f}")
            insights.append(f"- Average: {stats['mean']:.2f}")
            insights.append(f"- Median: {stats['50%']:.2f}")
        
        return insights

    def ask_question(self, df: pd.DataFrame, question: str) -> str:
        """Answer questions about the dataset using OpenAI"""
        try:
            # Prepare context with dataset information
            context = f"""
            Dataset Summary:
            - Total rows: {len(df)}
            - Columns: {', '.join(df.columns)}
            - Numerical statistics:\n{df.describe().to_string()}
            - Sample data:\n{df.head().to_string()}
            """

            # Create prompt
            prompt = f"""
            Based on this dataset information:
            {context}
            
            Question: {question}
            
            Provide a clear, concise answer using only the data available.
            """

            # Get response from OpenAI using older API style
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a data analyst assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )

            return response.choices[0].message['content']

        except Exception as e:
            return f"Error analyzing data: {str(e)}"

def create_analysis_page(df: pd.DataFrame, analyzer: DataAnalyzer):
    """Create the analysis page in Streamlit"""
    
    # Add anchor for navigation
    st.markdown('<a name="qa_section"></a>', unsafe_allow_html=True)
    
    if analyzer.llm_available:
        # Run anomaly detection in background
        anomalies = analyzer.detect_anomalies(df)
        
        # Clean column names - remove (generated) suffix
        def clean_column_name(col):
            return col.replace(" (generated)", "").strip()
        
        # Generate smart questions based on anomalies
        smart_questions = []
        
        # Add questions about detected anomalies
        for col, data in anomalies.items():
            clean_col = clean_column_name(col)
            if data['count'] > 0:
                smart_questions.extend([
                    f"Can you analyze the unusual values found in the {clean_col} column?",
                    f"What patterns do you see in the {clean_col} outliers?",
                    f"How do the {clean_col} anomalies affect the overall distribution?"
                ])
        
        # Add questions about correlations
        num_cols = df.select_dtypes(include=[np.number]).columns
        if len(num_cols) > 1:
            corr_matrix = df[num_cols].corr()
            for i in range(len(num_cols)):
                for j in range(i+1, len(num_cols)):
                    if abs(corr_matrix.iloc[i,j]) > 0.5:
                        col1 = clean_column_name(num_cols[i])
                        col2 = clean_column_name(num_cols[j])
                        smart_questions.append(
                            f"What insights can we draw from the relationship between {col1} and {col2}?"
                        )
        
        # Add general analytical questions
        smart_questions.extend([
            "What are the key trends and patterns in this dataset?",
            "Can you provide a summary of the main statistical findings?",
            "What are the most interesting insights from this data?",
            "Are there any notable correlations or relationships in the data?",
            "What conclusions can we draw from the numerical distributions?"
        ])
        
        st.subheader("💬 Ask Questions About Your Data")
        
        # Show AI-generated questions first
        question = st.selectbox(
            "Select a question or type your own below:",
            [""] + list(set(smart_questions)),  # Remove duplicates
            key="question_select"
        )
        
        custom_question = st.text_input(
            "Or type your own question:",
            key="custom_question",
            help="Ask anything about the data and I'll analyze it for you"
        )
        
        if st.button("Get Answer", key="get_answer"):
            final_question = custom_question if custom_question else question
            if final_question:
                with st.spinner("Analyzing..."):
                    answer = analyzer.ask_question(df, final_question)
                    st.markdown(f"**Answer:** {answer}")

def initialize_analyzer():
    """Initialize the data analyzer"""
    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = DataAnalyzer()

def show_analysis_tab(df: pd.DataFrame):
    """Show analysis tab for the current dataset"""
    initialize_analyzer()
    create_analysis_page(df, st.session_state.analyzer) 