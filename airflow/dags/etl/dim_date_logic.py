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

class DateETL(BaseETL):
    """ETL process for the date dimension table."""
    
    def __init__(self):
        super().__init__()
    
    def get_table_name(self) -> str:
        """Returns the target table name."""
        return 'dim_date'
    
    def extract_data(self) -> pd.DataFrame:
        """Extracts date data from rental table."""
        date_df = pd.read_sql("""
            SELECT rental_date
            FROM rental
        """, con=self.source_engine)
        return date_df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforms date data for the data warehouse schema."""
        df['rental_date'] = pd.to_datetime(df['rental_date'])
    
        df['week'] = df['rental_date'].dt.isocalendar().week
        df['rental_month'] = df['rental_date'].dt.month
        
        df['rental_date'] = df['rental_date'].dt.date
        
        df = df.drop_duplicates(subset='rental_date')
        print(df)
        df['date_key'] = range(1, len(df) + 1)

        df.rename(columns={'rental_date': 'full_date', 'week': 'rental_week'}, inplace=True)    
        return df