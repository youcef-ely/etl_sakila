import abc
import pandas as pd
from src import get_config, create_db_engine



class BaseETL():

    def __init__(self):
        self.database_config = get_config('sakila')
        self.warehouse_config = get_config('sakila_dw')
        
        self.source_engine = create_db_engine(self.database_config)
        self.warehouse_engine = create_db_engine(self.warehouse_config) 
    
    @abc.abstractmethod
    def extract_data(self) -> pd.DataFrame:
        pass


    @abc.abstractmethod    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


    @abc.abstractmethod
    def load_data(self, df: pd.DataFrame, table_name) -> None:
        """Loads transformed data into the data warehouse."""
        try:
            df.to_sql(name=table_name, con=self.warehouse_engine, if_exists='append', index=False)
        except Exception as e:
            raise