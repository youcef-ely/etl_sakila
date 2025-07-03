import os
import logging
from typing import Dict
from .config import Config
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)




def get_config(db_name: str = 'sakila') -> Dict[str, str | int]:
    """
    Get database configuration from config.py file.
    
    Args:
        db_name: Database name ('sakila' for source, 'sakila_dw' for warehouse)
        
    Returns:
        Dictionary containing database connection parameters
        
    Raises:
        ValueError: If unknown database name is provided
    """
    source_db_config = {
        "user": Config.MYSQL_SOURCE_USER,
        "password": Config.MYSQL_SOURCE_ROOT_PASSWORD,
        "host": Config.MYSQL_SOURCE_HOST,
        "port": Config.MYSQL_SOURCE_PORT,
        "database": Config.MYSQL_SOURCE_DATABASE,
    }

    warehouse_db_config = {
        "user": Config.MYSQL_WAREHOUSE_USER,
        "password": Config.MYSQL_WAREHOUSE_ROOT_PASSWORD,
        "host": Config.MYSQL_WAREHOUSE_HOST,
        "port": Config.MYSQL_WAREHOUSE_PORT,
        "database": Config.MYSQL_WAREHOUSE_DATABASE,
    }

    if db_name == 'sakila':
        return source_db_config
    elif db_name == 'sakila_dw':
        return warehouse_db_config
    else:
        raise ValueError(f"Unknown database name: {db_name}. Use 'sakila' or 'sakila_dw'")


def create_db_engine(config_dict: dict):
    try:
        db_url = f"mysql+pymysql://{config_dict['user']}:{config_dict['password']}@{config_dict['host']}:{config_dict['port']}/{config_dict['database']}"
        engine = create_engine(db_url)

        # Just test the connection without executing a query
        connection = engine.connect()
        connection.close()

        logger.info(f"Successfully connected to database `{config_dict['database']}` at {config_dict['host']}:{config_dict['port']}")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database `{config_dict['database']}` at {config_dict['host']}:{config_dict['port']}: {e}")
        raise


def remove_file_safely(file_path: str) -> None:
    """
    Remove file if it exists, with error handling.
    
    Args:
        file_path: Path to the file to be removed
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Successfully removed file: {file_path}")
        else:
            logger.debug(f"File does not exist, no need to remove: {file_path}")
    except Exception as e:
        logger.warning(f"Could not remove file {file_path}: {e}")