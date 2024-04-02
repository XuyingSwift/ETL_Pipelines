import requests
import sqlite3
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        logger.info(f'{parquet_file_name} : extracted {df_parquet.shape[0]} records from the parquet file')
    except Exception as e:
        logger.exception(f'{parquet_file_name}: - exception {e} encountered while extracting the parquet file')
        df_parquet = pd.DataFrame()
    return df_parquet

def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
        logger.info(f'{csv_file_name} : extracted {df_csv.shape[0]} records from csv file')
    except Exception as e:
        logger.exception(f'{csv_file_name}: - exception {e} encountered while extracting the csv file')
        df_csv = pd.DataFrame()
    return df_csv

def source_data_from_api(api_endpoint):
    try:
        response = requests.get(api_endpoint)
        if response.status_code == 200:
            logger.info(f'{response.status_code} - ok: while invoking the api {api_endpoint}')
            data = response.json()
            df_api = pd.json_normalize(data)
            logger.info(f'api_endpoint - extracted {df_api.shape[0]} records from the csv file')
        else:
            logger.error(f'{response.status_code} - error: while invoking the api {api_endpoint}')
            df_api = pd.DataFrame()
    except Exception as e:
        logger.exception(f'exception {e} encountered while reading data from api')
        df_api = pd.DataFrame()
    return df_api

def source_data_from_table(db_name, table_name):
    try:
        if not table_name.isidentifier():
            raise ValueError("Invalid table name.")
        query = f'SELECT * FROM {table_name}'
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql_query(query, conn)
            logger.info(f'{db_name} - read {df_table.shape[0]} records from the table: {table_name}')
    except Exception as e:
        print(f'Failed to read from table {table_name} in database {db_name}: {e}')
        logger.exception(f'{db_name} : - exception {e} encountered while reading data from the table: {table_name}')
        df_table = pd.DataFrame()
    return df_table

def source_data_from_webpage(web_page_url, matching_keyword):
    try:
        html_data = pd.read_html(web_page_url, match=matching_keyword)
        if html_data:
            df_html = df_html[0]
            logger.info(f'{web_page_url} - read {df_html.shape[0]} records from the page: {web_page_url}')
        else:
            print(f'No data found matching keyword "{matching_keyword}" on the web page.')
            df_html = pd.DataFrame()
    except Exception as e:
        print(f'Failed to read from web page {web_page_url} with keyword "{matching_keyword}": {e}')
        logger.exception(f'{web_page_url} : - exception {e} encountered while reading data from the page: {web_page_url}')
        df_html = pd.DataFrame()
    return df_html

def extracted_data():
    parquet_file_name = "/Users/xuyingwangswift/Desktop/ETL_Pipelines/data/yellow_tripdata_2022-01.parquet"
    csv_file_name = "/Users/xuyingwangswift/Desktop/ETL_Pipelines/data/h9gi-nx95.csv"
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    db_name = "movies.sqlite"
    table_name = "movies"
    web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    matching_keyword = "by country"

    df_parquet = source_data_from_parquet(parquet_file_name),
                                                     
    return df_parquet
