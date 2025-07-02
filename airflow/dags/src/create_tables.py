import pretty_errors
from models import Base
from helpers import get_config, create_db_engine





config = get_config('sakila_dw')


def recreate_datawarehouse(config: dict):
    engine = create_db_engine(config)

    print(f"Dropping all tables in database: {config['database']}")
    Base.metadata.drop_all(engine)

    print(f"Creating all tables in database: {config['database']}")
    Base.metadata.create_all(engine)

    print(f"Schema refreshed in database: {config['database']}")

if __name__ == "__main__":
    recreate_datawarehouse(config)