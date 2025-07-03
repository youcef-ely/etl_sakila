import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from airflow.dags.src import get_config, create_db_engine


# Configure Streamlit page
st.set_page_config(
    page_title="Rental Analytics Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

config = get_config('sakila_dw')
config['port'] = 3307
config['host'] = 'localhost'  # Use Docker service name for MySQL

# Cache data loading functions
@st.cache_data
def load_kpi_data():
    """Load key performance indicators"""
    engine = create_db_engine(config)
    
    query = """
    SELECT 
        COUNT(*) as total_rentals,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_rental_amount,
        COUNT(DISTINCT customer_key) as unique_customers,
        COUNT(DISTINCT film_key) as unique_films,
        COUNT(DISTINCT store_key) as unique_stores
    FROM fact_rental
    """
    
    return pd.read_sql(query, engine)

@st.cache_data
def load_revenue_by_month():
    """Load revenue trends by month"""
    engine = create_db_engine(config)
    
    query = """
    SELECT 
        dd.rental_month,
        dd.full_date,
        SUM(fr.amount) as monthly_revenue,
        COUNT(fr.rental_key) as monthly_rentals
    FROM fact_rental fr
    JOIN dim_date dd ON fr.date_key = dd.date_key
    GROUP BY dd.rental_month, dd.full_date
    ORDER BY dd.full_date
    """
    
    return pd.read_sql(query, engine)

@st.cache_data
def load_revenue_by_store():
    """Load revenue by store location"""
    engine = create_db_engine(config)
    
    query = """
    SELECT 
        ds.city,
        ds.state,
        ds.country,
        SUM(fr.amount) as store_revenue,
        COUNT(fr.rental_key) as store_rentals
    FROM fact_rental fr
    JOIN dim_store ds ON fr.store_key = ds.store_key
    GROUP BY ds.store_key, ds.city, ds.state, ds.country
    ORDER BY store_revenue DESC
    """
    
    return pd.read_sql(query, engine)

@st.cache_data
def load_top_films():
    """Load top performing films"""
    engine = create_db_engine(config)
    
    query = """
    SELECT 
        df.title,
        df.category,
        df.length,
        SUM(fr.amount) as film_revenue,
        COUNT(fr.rental_key) as rental_count
    FROM fact_rental fr
    JOIN dim_film df ON fr.film_key = df.film_key
    GROUP BY df.film_key, df.title, df.category, df.length
    ORDER BY film_revenue DESC
    LIMIT 20
    """
    
    return pd.read_sql(query, engine)

@st.cache_data
def load_customer_analysis():
    """Load customer segmentation data"""
    engine = create_db_engine(config)

    query = """
    SELECT 
        dc.city,
        dc.state,
        dc.country,
        COUNT(DISTINCT dc.customer_key) as customer_count,
        SUM(fr.amount) as customer_revenue,
        AVG(fr.amount) as avg_customer_value
    FROM fact_rental fr
    JOIN dim_client dc ON fr.customer_key = dc.customer_key
    GROUP BY dc.city, dc.state, dc.country
    ORDER BY customer_revenue DESC
    """
    
    return pd.read_sql(query, engine)

@st.cache_data
def load_category_performance():
    """Load film category performance"""
    engine = create_db_engine(config)

    query = """
    SELECT 
        df.category,
        COUNT(fr.rental_key) as rental_count,
        SUM(fr.amount) as category_revenue,
        AVG(fr.amount) as avg_rental_price
    FROM fact_rental fr
    JOIN dim_film df ON fr.film_key = df.film_key
    GROUP BY df.category
    ORDER BY category_revenue DESC
    """
    
    return pd.read_sql(query, engine)

def main():
    st.title("🎬 Rental Analytics Dashboard")
    st.markdown("---")
    
    # Sidebar filters
    st.sidebar.header("📊 Dashboard Filters")
    
    # Load data
    try:
        kpi_data = load_kpi_data()
        revenue_by_month = load_revenue_by_month()
        revenue_by_store = load_revenue_by_store()
        top_films = load_top_films()
        customer_analysis = load_customer_analysis()
        category_performance = load_category_performance()
        
        # Sidebar filters
        categories = ['All'] + list(category_performance['category'].unique())
        selected_category = st.sidebar.selectbox("Filter by Category", categories)
        
        countries = ['All'] + list(revenue_by_store['country'].unique())
        selected_country = st.sidebar.selectbox("Filter by Store Country", countries)
        
        # KPI Section
        st.header("📈 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Revenue",
                value=f"${kpi_data['total_revenue'].iloc[0]:,.2f}"
            )
        
        with col2:
            st.metric(
                label="Total Rentals",
                value=f"{kpi_data['total_rentals'].iloc[0]:,}"
            )
        
        with col3:
            st.metric(
                label="Average Rental",
                value=f"${kpi_data['avg_rental_amount'].iloc[0]:.2f}"
            )
        
        with col4:
            st.metric(
                label="Unique Customers",
                value=f"{kpi_data['unique_customers'].iloc[0]:,}"
            )
        
        st.markdown("---")
        
        # Revenue Trends
        st.header("📊 Revenue Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_revenue = px.line(
                revenue_by_month,
                x='full_date',
                y='monthly_revenue',
                title='Monthly Revenue Trend',
                labels={'monthly_revenue': 'Revenue ($)', 'full_date': 'Date'}
            )
            fig_revenue.update_layout(height=400)
            st.plotly_chart(fig_revenue, use_container_width=True)
        
        with col2:
            fig_rentals = px.bar(
                revenue_by_month,
                x='rental_month',
                y='monthly_rentals',
                title='Monthly Rental Count',
                labels={'monthly_rentals': 'Number of Rentals', 'rental_month': 'Month'}
            )
            fig_rentals.update_layout(height=400)
            st.plotly_chart(fig_rentals, use_container_width=True)
        
        st.markdown("---")
        
        # Store Performance
        st.header("🏪 Store Performance")
        
        # Filter by country if selected
        store_data = revenue_by_store
        if selected_country != 'All':
            store_data = store_data[store_data['country'] == selected_country]
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_store_revenue = px.bar(
                store_data.head(10),
                x='store_revenue',
                y='city',
                orientation='h',
                title='Top 10 Stores by Revenue',
                labels={'store_revenue': 'Revenue ($)', 'city': 'City'}
            )
            fig_store_revenue.update_layout(height=400)
            st.plotly_chart(fig_store_revenue, use_container_width=True)
        
        with col2:
            fig_store_map = px.scatter_geo(
                store_data,
                locations='country',
                locationmode='country names',
                size='store_revenue',
                hover_name='city',
                title='Revenue by Geographic Location',
                size_max=50
            )
            fig_store_map.update_layout(height=400)
            st.plotly_chart(fig_store_map, use_container_width=True)
        
        st.markdown("---")
        
        # Film Analysis
        st.header("🎭 Film Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter films by category if selected
            film_data = top_films
            if selected_category != 'All':
                film_data = film_data[film_data['category'] == selected_category]
            
            fig_top_films = px.bar(
                film_data.head(10),
                x='film_revenue',
                y='title',
                orientation='h',
                title=f'Top 10 Films by Revenue{" - " + selected_category if selected_category != "All" else ""}',
                labels={'film_revenue': 'Revenue ($)', 'title': 'Film Title'}
            )
            fig_top_films.update_layout(height=500)
            st.plotly_chart(fig_top_films, use_container_width=True)
        
        with col2:
            fig_category = px.pie(
                category_performance,
                values='category_revenue',
                names='category',
                title='Revenue Distribution by Category'
            )
            fig_category.update_layout(height=500)
            st.plotly_chart(fig_category, use_container_width=True)
        
        st.markdown("---")
        
        # Customer Analysis
        st.header("👥 Customer Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter customers by country if selected
            customer_data = customer_analysis
            if selected_country != 'All':
                customer_data = customer_data[customer_data['country'] == selected_country]
            
            fig_customer_revenue = px.scatter(
                customer_data,
                x='customer_count',
                y='customer_revenue',
                size='avg_customer_value',
                hover_name='city',
                title='Customer Count vs Revenue by City',
                labels={
                    'customer_count': 'Number of Customers',
                    'customer_revenue': 'Total Revenue ($)'
                }
            )
            fig_customer_revenue.update_layout(height=400)
            st.plotly_chart(fig_customer_revenue, use_container_width=True)
        
        with col2:
            fig_avg_customer = px.bar(
                customer_data.head(10),
                x='avg_customer_value',
                y='city',
                orientation='h',
                title='Average Customer Value by City',
                labels={'avg_customer_value': 'Average Customer Value ($)', 'city': 'City'}
            )
            fig_avg_customer.update_layout(height=400)
            st.plotly_chart(fig_avg_customer, use_container_width=True)
        
        # Data Tables
        st.markdown("---")
        st.header("📋 Detailed Data")
        
        tab1, tab2, tab3, tab4 = st.tabs(["Top Films", "Store Performance", "Customer Analysis", "Category Performance"])
        
        with tab1:
            st.dataframe(top_films, use_container_width=True)
        
        with tab2:
            st.dataframe(revenue_by_store, use_container_width=True)
        
        with tab3:
            st.dataframe(customer_analysis, use_container_width=True)
        
        with tab4:
            st.dataframe(category_performance, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.info("Please ensure your database is running and accessible.")
        st.code("""
        # Make sure your database connection is correct:
        # - Host: mysql-warehouse (if running in Docker)
        # - Port: 3306 (internal Docker port)
        # - Database: sakila_dw
        # - User: root
        # - Password: warehousepass
        """)

if __name__ == "__main__":
    main()