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

class CustomerETL(BaseETL):
    """ETL process for the customer dimension table."""

    def __init__(self):
        super().__init__()

    
    def get_table_name(self) -> str:
        """Returns the target table name."""
        return 'dim_client'

    def extract_data(self) -> pd.DataFrame:
        """Extracts customer and address-related tables and merges them into one DataFrame."""
        try:
            logger.info("Starting customer data extraction...")
            
            # Extract customer data
            customer_df = pd.read_sql("""
                SELECT customer_id, store_id, first_name, last_name, email, address_id
                FROM customer
            """, con=self.source_engine)
            logger.info(f"Extracted {len(customer_df)} customer records")

            # Extract address data
            address_df = pd.read_sql("""
                SELECT address_id, city_id, district
                FROM address
            """, con=self.source_engine)
            logger.info(f"Extracted {len(address_df)} address records")

            # Extract city data
            city_df = pd.read_sql("""
                SELECT city_id, city, country_id
                FROM city
            """, con=self.source_engine)
            logger.info(f"Extracted {len(city_df)} city records")

            # Extract country data
            country_df = pd.read_sql("""
                SELECT country_id, country
                FROM country
            """, con=self.source_engine)
            logger.info(f"Extracted {len(country_df)} country records")

            # Merge all data
            full_address = (address_df
                           .merge(city_df, on='city_id')
                           .merge(country_df, on='country_id'))
            
            merged_df = customer_df.merge(full_address, on='address_id')
            logger.info(f"Successfully merged data: {len(merged_df)} final records")
            
            return merged_df
            
        except Exception as e:
            logger.error(f"Failed to extract customer data: {e}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforms customer data for the data warehouse schema."""
        try:
            logger.info("Starting customer data transformation...")
            
            # Create full name
            df['full_name'] = df['first_name'] + ' ' + df['last_name']
            
            # Drop unnecessary columns
            df.drop(['first_name', 'last_name', 'city_id', 'country_id', 'address_id'], 
                   axis=1, inplace=True)
            
            # Rename columns for warehouse schema
            df.rename(columns={
                'customer_id': 'customer_key',
                'district': 'state'
            }, inplace=True)
            
            # Add any additional transformations here
            df['email'] = df['email'].str.lower().str.strip()  # Clean email
            
            logger.info("Customer data transformation completed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Failed to transform customer data: {e}")
            raise

