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

class FilmETL(BaseETL):
    """ETL process for the film dimension table."""

    def __init__(self):
        super().__init__()

    def get_table_name(self) -> str:
        """Returns the target table name."""
        return 'dim_film'

    def extract_data(self) -> pd.DataFrame:
        """Extracts film data from film, film_category, and category tables."""
        film_df = pd.read_sql("""
            SELECT film_id, title, description, length
            FROM film
        """, con=self.source_engine)

        film_cat_df = pd.read_sql("""
            SELECT film_id, category_id
            FROM film_category
        """, con=self.source_engine)

        category_df = pd.read_sql("""
            SELECT category_id, name
            FROM category
        """, con=self.source_engine)

        merged_df = film_df.merge(film_cat_df, on='film_id').merge(category_df, on='category_id')
        return merged_df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforms film data for the data warehouse schema."""
        df.drop(['category_id'], axis=1, inplace=True)
        df.rename(columns={
            'name': 'category',
        }, inplace=True)

        df.rename(columns={'film_id': 'film_key'}, inplace=True)
        return df

