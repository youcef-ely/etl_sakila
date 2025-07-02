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

class StoreETL(BaseETL):
    """ETL process for the store dimension table."""

    def __init__(self):
        super().__init__()

    def get_table_name(self) -> str:
        """Returns the target table name."""
        return 'dim_store'

    def extract_data(self) -> pd.DataFrame:
        """Extracts store data from store, address, city, and country tables."""
        store_df = pd.read_sql("""
            SELECT store_id, address_id
            FROM store
        """, con=self.source_engine)

        address_df = pd.read_sql("""
            SELECT address_id, address, city_id, district
            FROM address
        """, con=self.source_engine)

        city_df = pd.read_sql("""
            SELECT city_id, city, country_id
            FROM city
        """, con=self.source_engine)

        country_df = pd.read_sql("""
            SELECT country_id, country
            FROM country
        """, con=self.source_engine)

        full_address = address_df.merge(city_df, on='city_id').merge(country_df, on='country_id')
        merged_df = store_df.merge(full_address, on='address_id')
        return merged_df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforms store data for the data warehouse schema."""
        df.drop(['city_id', 'country_id', 'address_id'], axis=1, inplace=True)
        df.rename(columns={
            'store_id': 'store_key',
            'district': 'state'
        }, inplace=True)
        return df
