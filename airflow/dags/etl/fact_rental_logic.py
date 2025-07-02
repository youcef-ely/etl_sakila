import logging
import pandas as pd

from etl import BaseETL


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RentalETL(BaseETL):
    """ETL process for the rental fact table."""

    def __init__(self):
        super().__init__()

    def get_table_name(self) -> str:
        """Returns the target table name."""
        return 'fact_rental'

    def extract_data(self) -> pd.DataFrame:
        """Extract rental data with all necessary joins from source system"""
        logger.info("Extracting rental data from source system")
        
        query = """
            SELECT 
                r.rental_id,
                r.rental_date,
                r.customer_id,
                r.inventory_id,
                i.film_id,
                i.store_id,
                p.amount
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN payment p ON r.rental_id = p.rental_id
        """
        
        df = pd.read_sql(query, con=self.source_engine)
        logger.info(f"Extracted {len(df)} rental records")
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform rental data for the data warehouse schema."""
        df['rental_date'] = pd.to_datetime(df['rental_date']).dt.date
        
        # Get date keys
        dim_date = pd.read_sql("SELECT date_key, full_date FROM dim_date", con=self.warehouse_engine)
        dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date

        dim_customer = pd.read_sql("""
            SELECT customer_key, store_id as customer_store_id 
            FROM dim_client 
            ORDER BY customer_key
        """, con=self.warehouse_engine)
        
        # Get film keys - assuming dim_film has source_film_id or similar
        dim_film = pd.read_sql("""
            SELECT film_key, title 
            FROM dim_film 
            ORDER BY film_key
        """, con=self.warehouse_engine)
        
        # Get store keys
        dim_store = pd.read_sql("""
            SELECT store_key, address, city 
            FROM dim_store 
            ORDER BY store_key
        """, con=self.warehouse_engine)

        # Date lookup
        df = df.merge(dim_date, left_on='rental_date', right_on='full_date', how='left')

        df['customer_key'] = df['customer_id']  # This assumes 1:1 mapping
        
        # Film lookup - assuming film_key corresponds to film_id
        df['film_key'] = df['film_id']  # This assumes 1:1 mapping
        
        # Store lookup - assuming store_key corresponds to store_id
        df['store_key'] = df['store_id']  # This assumes 1:1 mapping
        
        # Select final columns for fact table
        fact_columns = ['customer_key', 'film_key', 'store_key', 'date_key', 'amount']
        fact_df = df[fact_columns].copy()
        
        # Remove records with missing dimension keys
        initial_count = len(fact_df)
        fact_df.dropna(subset=['customer_key', 'film_key', 'store_key', 'date_key'], inplace=True)
        final_count = len(fact_df)
        
        if initial_count != final_count:
            logger.warning(f"Dropped {initial_count - final_count} records due to missing dimension keys")
        
        # Ensure proper data types
        fact_df = fact_df.astype({
            'customer_key': 'int',
            'film_key': 'int',
            'store_key': 'int',
            'date_key': 'int',
            'amount': 'float'
        })
        
        logger.info(f"Transformation complete. Final record count: {len(fact_df)}")

        fact_df['rental_key'] = list(range(1, len(fact_df)+1))
        return fact_df
    
