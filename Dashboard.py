"""
Real-Time Customer Heartbeat Monitoring Dashboard

A Streamlit-based dashboard for visualizing heartbeat data and monitoring anomalies.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import time

# Page configuration
st.set_page_config(
    page_title="Heartbeat Monitor Dashboard",
    page_icon="❤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database configuration
import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'heartbeat_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}


@st.cache_resource
def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def fetch_recent_data(hours=1):
    """Fetch recent heartbeat data"""
    conn = get_db_connection()
    query = f"""
        SELECT 
            customer_id,
            recorded_at,
            heart_rate,
            anomaly_level,
            anomaly_description
        FROM heartbeat_records
        WHERE recorded_at >= NOW() - INTERVAL '{hours} hours'
        ORDER BY recorded_at DESC
    """
    df = pd.read_sql(query, conn)
    df['recorded_at'] = pd.to_datetime(df['recorded_at'])
    return df


def fetch_customer_stats():
    """Fetch customer statistics"""
    conn = get_db_connection()
    query = """
        SELECT * FROM customer_stats
        ORDER BY total_readings DESC
    """
    df = pd.read_sql(query, conn)
    return df


def fetch_critical_anomalies(hours=24):
    """Fetch critical anomalies"""
    conn = get_db_connection()
    query = f"""
        SELECT 
            customer_id,
            recorded_at,
            heart_rate,
            anomaly_level,
            anomaly_description
        FROM heartbeat_records
        WHERE anomaly_level IN ('critical_low', 'critical_high')
            AND recorded_at >= NOW() - INTERVAL '{hours} hours'
        ORDER BY recorded_at DESC
    """
    df = pd.read_sql(query, conn)
    df['recorded_at'] = pd.to_datetime(df['recorded_at'])
    return df


def main():
    """Main dashboard function"""
    
    # Title and header
    st.title("❤️ Real-Time Customer Heartbeat Monitoring Dashboard")
    st.markdown("---")
    
    # Sidebar controls
    st.sidebar.header("⚙️ Dashboard Controls")
    
    # Time range selector
    time_range = st.sidebar.selectbox(
        "Time Range",
        options=[1, 3, 6, 12, 24],
        format_func=lambda x: f"Last {x} hour{'s' if x > 1 else ''}",
        index=3
    )
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Fetch data
    with st.spinner("Loading data..."):
        df_recent = fetch_recent_data(hours=time_range)
        df_stats = fetch_customer_stats()
        df_critical = fetch_critical_anomalies(hours=24)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_readings = len(df_recent)
        st.metric("Total Readings", f"{total_readings:,}")
    
    with col2:
        active_customers = df_recent['customer_id'].nunique()
        st.metric("Active Customers", active_customers)
    
    with col3:
        avg_heart_rate = df_recent['heart_rate'].mean() if len(df_recent) > 0 else 0
        st.metric("Avg Heart Rate", f"{avg_heart_rate:.1f} bpm")
    
    with col4:
        critical_count = len(df_critical)
        st.metric("Critical Alerts (24h)", critical_count, delta_color="inverse")
    
    st.markdown("---")
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview", 
        "👥 Customer Analysis", 
        "⚠️ Anomaly Detection",
        "📈 Trends"
    ])
    
    # Tab 1: Overview
    with tab1:
        st.header("Real-Time Heart Rate Overview")
        
        if len(df_recent) > 0:
            # Time series plot for all customers
            fig = px.line(
                df_recent.sort_values('recorded_at'),
                x='recorded_at',
                y='heart_rate',
                color='customer_id',
                title=f"Heart Rate Over Time (Last {time_range} hour{'s' if time_range > 1 else ''})",
                labels={'heart_rate': 'Heart Rate (bpm)', 'recorded_at': 'Time'}
            )
            
            # Add threshold lines
            fig.add_hline(y=60, line_dash="dash", line_color="green", 
                         annotation_text="Normal Low (60)")
            fig.add_hline(y=100, line_dash="dash", line_color="orange", 
                         annotation_text="Normal High (100)")
            fig.add_hline(y=40, line_dash="dot", line_color="red", 
                         annotation_text="Critical Low (40)")
            fig.add_hline(y=160, line_dash="dot", line_color="red", 
                         annotation_text="Critical High (160)")
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Distribution plot
            col1, col2 = st.columns(2)
            
            with col1:
                fig_dist = px.histogram(
                    df_recent,
                    x='heart_rate',
                    nbins=30,
                    title="Heart Rate Distribution",
                    labels={'heart_rate': 'Heart Rate (bpm)', 'count': 'Frequency'}
                )
                st.plotly_chart(fig_dist, use_container_width=True)
            
            with col2:
                # Anomaly level distribution
                anomaly_counts = df_recent['anomaly_level'].value_counts()
                fig_anomaly = px.pie(
                    values=anomaly_counts.values,
                    names=anomaly_counts.index,
                    title="Anomaly Level Distribution",
                    color_discrete_map={
                        'normal': 'green',
                        'elevated': 'yellow',
                        'low': 'orange',
                        'high': 'orange',
                        'critical_low': 'red',
                        'critical_high': 'red'
                    }
                )
                st.plotly_chart(fig_anomaly, use_container_width=True)
        else:
            st.info(f"No data available for the last {time_range} hour(s)")
    
    # Tab 2: Customer Analysis
    with tab2:
        st.header("Individual Customer Analysis")
        
        if len(df_stats) > 0:
            # Customer selector
            selected_customer = st.selectbox(
                "Select Customer",
                options=df_stats['customer_id'].tolist()
            )
            
            # Customer data
            customer_data = df_recent[df_recent['customer_id'] == selected_customer]
            customer_stats = df_stats[df_stats['customer_id'] == selected_customer].iloc[0]
            
            # Display customer stats
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Readings", f"{int(customer_stats['total_readings']):,}")
            with col2:
                st.metric("Avg Heart Rate", f"{customer_stats['avg_heart_rate']:.1f} bpm")
            with col3:
                st.metric("Min/Max", f"{int(customer_stats['min_heart_rate'])}/{int(customer_stats['max_heart_rate'])}")
            with col4:
                st.metric("Critical Events", int(customer_stats['critical_count']))
            
            if len(customer_data) > 0:
                # Customer heart rate over time
                fig = px.scatter(
                    customer_data.sort_values('recorded_at'),
                    x='recorded_at',
                    y='heart_rate',
                    color='anomaly_level',
                    title=f"Heart Rate Timeline - {selected_customer}",
                    labels={'heart_rate': 'Heart Rate (bpm)', 'recorded_at': 'Time'},
                    color_discrete_map={
                        'normal': 'green',
                        'elevated': 'yellow',
                        'low': 'orange',
                        'high': 'orange',
                        'critical_low': 'red',
                        'critical_high': 'red'
                    }
                )
                fig.update_traces(marker=dict(size=10))
                st.plotly_chart(fig, use_container_width=True)
                
                # Recent readings table
                st.subheader("Recent Readings")
                display_df = customer_data[['recorded_at', 'heart_rate', 'anomaly_level', 'anomaly_description']].head(20)
                st.dataframe(display_df, use_container_width=True)
            else:
                st.info(f"No data for {selected_customer} in the selected time range")
        else:
            st.info("No customer data available")
    
    # Tab 3: Anomaly Detection
    with tab3:
        st.header("Critical Anomaly Alerts")
        
        if len(df_critical) > 0:
            st.warning(f"⚠️ {len(df_critical)} critical anomalies detected in the last 24 hours")
            
            # Critical events timeline
            fig = px.scatter(
                df_critical.sort_values('recorded_at'),
                x='recorded_at',
                y='heart_rate',
                color='customer_id',
                symbol='anomaly_level',
                size='heart_rate',
                title="Critical Anomaly Timeline (Last 24 Hours)",
                labels={'heart_rate': 'Heart Rate (bpm)', 'recorded_at': 'Time'}
            )
            fig.update_traces(marker=dict(size=15))
            st.plotly_chart(fig, use_container_width=True)
            
            # Critical events table
            st.subheader("Critical Event Details")
            display_critical = df_critical[['customer_id', 'recorded_at', 'heart_rate', 'anomaly_level', 'anomaly_description']]
            st.dataframe(
                display_critical.style.apply(
                    lambda x: ['background-color: #ffcccc' if x['anomaly_level'] in ['critical_low', 'critical_high'] else '' for i in x],
                    axis=1
                ),
                use_container_width=True
            )
        else:
            st.success("✅ No critical anomalies detected in the last 24 hours")
        
        # All anomalies in time range
        anomalies = df_recent[df_recent['anomaly_level'] != 'normal']
        if len(anomalies) > 0:
            st.subheader(f"All Anomalies (Last {time_range} hour{'s' if time_range > 1 else ''})")
            st.dataframe(
                anomalies[['customer_id', 'recorded_at', 'heart_rate', 'anomaly_level', 'anomaly_description']],
                use_container_width=True
            )
    
    # Tab 4: Trends
    with tab4:
        st.header("Heart Rate Trends")
        
        if len(df_recent) > 0:
            # Hourly aggregation
            df_hourly = df_recent.copy()
            df_hourly['hour'] = df_hourly['recorded_at'].dt.floor('H')
            hourly_stats = df_hourly.groupby('hour').agg({
                'heart_rate': ['mean', 'min', 'max', 'count']
            }).reset_index()
            hourly_stats.columns = ['hour', 'avg_hr', 'min_hr', 'max_hr', 'count']
            
            # Hourly trend plot
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=("Average Heart Rate by Hour", "Reading Count by Hour"),
                vertical_spacing=0.15
            )
            
            fig.add_trace(
                go.Scatter(x=hourly_stats['hour'], y=hourly_stats['avg_hr'], 
                          name='Avg HR', line=dict(color='blue')),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=hourly_stats['hour'], y=hourly_stats['min_hr'], 
                          name='Min HR', line=dict(color='green', dash='dash')),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=hourly_stats['hour'], y=hourly_stats['max_hr'], 
                          name='Max HR', line=dict(color='red', dash='dash')),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(x=hourly_stats['hour'], y=hourly_stats['count'], 
                      name='Count', marker_color='lightblue'),
                row=2, col=1
            )
            
            fig.update_xaxes(title_text="Hour", row=2, col=1)
            fig.update_yaxes(title_text="Heart Rate (bpm)", row=1, col=1)
            fig.update_yaxes(title_text="Reading Count", row=2, col=1)
            fig.update_layout(height=700, showlegend=True)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Customer comparison
            st.subheader("Customer Comparison")
            customer_avg = df_recent.groupby('customer_id')['heart_rate'].agg(['mean', 'std']).reset_index()
            customer_avg.columns = ['customer_id', 'avg_heart_rate', 'std_heart_rate']
            
            fig = px.bar(
                customer_avg.sort_values('avg_heart_rate'),
                x='customer_id',
                y='avg_heart_rate',
                error_y='std_heart_rate',
                title="Average Heart Rate by Customer",
                labels={'avg_heart_rate': 'Average Heart Rate (bpm)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        if auto_refresh:
            st.caption("Auto-refreshing in 30s...")
            time.sleep(30)
            st.rerun()


if __name__ == "__main__":
    main()