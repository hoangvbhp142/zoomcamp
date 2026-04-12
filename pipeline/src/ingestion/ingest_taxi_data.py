import pandas as pd
from tqdm.auto import tqdm
from config.config_data import DATASET_CONFIG

def ingest_trip(engine, year, month):
	
	print("========== Ingesting yellow taxi data... ==========")

	url = DATASET_CONFIG["yellow_taxi_data"]["url"].format(year=year, month=month)
	parse_dates = DATASET_CONFIG["yellow_taxi_data"]["parse_dates"]
	dtype = DATASET_CONFIG["yellow_taxi_data"]["dtype"]
	table_name = DATASET_CONFIG["yellow_taxi_data"]["table_name"]

	df_iter = pd.read_csv(
		url,
		dtype=dtype,
		parse_dates=parse_dates,
		iterator=True,
		chunksize=100000
	)

	first = True

	for df_chunk in tqdm(df_iter):
		if first:
			df_chunk.head(0).to_sql(
				name=table_name,
				con=engine,
				index=False,
				if_exists='replace'
			)

		first = False

		df_chunk.to_sql(
			name=table_name,
			con=engine,
			index=False,
			if_exists='append'
		)