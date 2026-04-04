from sqlalchemy import create_engine
from ingest_taxi_data import ingest_trip
from ingest_zones_data import ingest_zone
import click

@click.command()
@click.option('--year', default=2021, type=int, help='Year to ingest')
@click.option('--month', default=1, type=int, help='Month to ingest')
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, help='PostgreSQL port')
def ingest_data(year, month, pg_user, pg_password, pg_db, pg_host, pg_port):
	engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
	ingest_zone(engine)
	ingest_trip(engine, year, month)
	
	print("========== Ingestion completed! ==========")

if __name__ == "__main__":
	ingest_data()
