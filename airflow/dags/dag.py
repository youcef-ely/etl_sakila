import os, sys
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from src import remove_file_safely
from etl import DateETL, FilmETL, StoreETL, CustomerETL, RentalETL



logger = logging.getLogger(__name__)

def etl_tasks(etl_class, base_file_name: str, ti):
    """
    Runs extract, transform, and load phases for a given ETL class.
    
    Parameters:
        etl_class: A subclass of BaseETL
        base_file_name (str): base file name (e.g. "rental") for temp parquet files
        ti: TaskInstance from Airflow to use XComs
    """
    etl = etl_class()

    raw_path = f"/opt/airflow/shared/{base_file_name}_raw.parquet"
    transformed_path = f"/opt/airflow/shared/{base_file_name}_transformed.parquet"

    try:
        logger.info("Starting extract phase")
        df_raw = etl.extract_data()
        df_raw.to_parquet(raw_path, index=False)
        ti.xcom_push(key="raw_data_path", value=raw_path)
        logger.info(f"Extracted {len(df_raw)} records to {raw_path}")

        # Transform
        logger.info("Starting transform phase")
        df_transformed = etl.transform_data(df_raw)
        df_transformed.to_parquet(transformed_path, index=False)
        ti.xcom_push(key="transformed_data_path", value=transformed_path)
        logger.info(f"Transformed data saved to {transformed_path}")

        # Load
        logger.info("Starting load phase")
        etl.load_data(df_transformed, table_name=etl.get_table_name())
        logger.info(f"Loaded data into `{etl.get_table_name()}`")

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:

        remove_file_safely(raw_path)
        remove_file_safely(transformed_path)
        etl.source_engine.dispose()
        etl.warehouse_engine.dispose()
        logger.info("Cleanup done.")





default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 2),
    'retries': 1
}

with DAG("rental_etl_dag",
         default_args=default_args,
         #schedule_interval="@weekly",
         catchup=False) as dag:

    date_etl_task = PythonOperator(
        task_id="run_date_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': DateETL,
            'base_file_name': 'date'
        }
    )

    film_etl_task = PythonOperator(
        task_id="run_film_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': FilmETL,
            'base_file_name': 'film'
        }
    )

    store_etl_task = PythonOperator(
        task_id="run_store_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': StoreETL,
            'base_file_name': 'store'
        }
    )

    customer_etl_task = PythonOperator(
        task_id="run_customer_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': CustomerETL,
            'base_file_name': 'customer'
        }
    )

    rental_etl_task = PythonOperator(
        task_id="run_rental_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': RentalETL,
            'base_file_name': 'rental'
        }
    )

    [date_etl_task, film_etl_task, store_etl_task, customer_etl_task] >> rental_etl_task