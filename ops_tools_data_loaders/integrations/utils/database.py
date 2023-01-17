import os
import sys
from datetime import datetime
from typing import Any

import mysql.connector
import pandas
import requests
import sqlparse
from numpy import array_split, nan
from sqlalchemy import create_engine, text

from integrations.utils.project_logger import logger


def mysql_db_connections(config: dict) -> dict[object, Any]:
    '''
    Creates MYSQL connection engine for schemas. It will also create the db schema, if it does not
    exist.
    
    :param: config: YAML config "MYSQL_DB"
    :return: dict {'schema_name': 'Connection engine'}
    '''
    # Create database/schemas if it does not exists
    conn = mysql.connector.connect(
        user=config['DB_USERNAME'],
        password=config['DB_PWD'],
        host=config['HOST']
    )

    for schema in config['SCHEMAS']:
        cursor = conn.cursor()
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS {schema};')

    conn.close()

    # Connection engine for each schema
    connections = {}

    for schema in config['SCHEMAS']:
        conn_url = f"mysql+pymysql://{config['DB_USERNAME']}:{config['DB_PWD']}@{config['HOST']}/{schema}"
        conn = create_engine(conn_url)
        connections[schema] = conn

        logger.info(f"Connection made for: {schema}")

    return connections


def run_sql_files(config: list, connection: dict[object, Any]) -> None:
    '''
    Executes sql scripts specified in the config YAML.
    
    :param: config: YAML config "SQL_QUERIES" list
    :param: connection: dict {'schema_name': 'Connection engine'}
    :return: None
    '''
    if not config:
        logger.info('Missing SQL Queries - Skip')
        return

    sql_queries = (file for file in config)

    for sql_query in sql_queries:
        sql_file = sql_query['SCRIPT']
        schema = sql_query['SCHEMA']
        sql_file_path = os.path.join(sys.path[0], "sql", sql_file)
        conn = connection[schema]

        with open(sql_file_path, 'r') as file:
            sql_raw = file.read()

        # Connection engine cannot execute sql with multiple queries, need to parse and execute
        # each query individually.
        queries = sqlparse.split(sqlparse.format(sql_raw, strip_comments=True))

        try:
            for query in queries:
                conn.execute(text(query))

            logger.info(f"SQL file ran sucessfully: {sql_file_path}")

        except Exception as e:
            logger.error(f"SQL File Error: {sql_file_path}: {e}")


def send_data_to_webhook(config: list, connection: dict[object, Any], env: str = 'stg') -> None:
    '''
    Sends data to webhooks. 
   
    :param: config: YAML config "WEBHOOKS" list
    :param: connection: dict {'schema_name': 'Connection engine'}
    :param: evn: YAML config "ENV" string "prod" | default "stg". Note: If env == 'stg', function
    will not execute. "ENV" must be set to "prod"
    :return: None
    '''
    if not config:
        logger.info('Missing Webhooks - Skip')
        return

    if env == 'stg':
        logger.info(
            f"Cannot Send Data - Env is set to 'stg'")
        return

    webhooks = (webhook for webhook in config)

    for webhook in webhooks:
        webhook_type = webhook['TYPE']
        webhook_url = webhook['URL']
        schema = webhook['SCHEMA']
        query = webhook['QUERY']
        num_of_payloads = webhook['NUM_OF_PAYLOADS']
        time = webhook['TIME']
        conn = connection[schema]

        # Adding this to reduce the number of operations being sent to the webhook.
        # This way we can avoid going over the limit for our integromat subscription.
        if time != 0 and int(datetime.now().strftime('%H%M')) < time:
            logger.info(
                f"Data not Sent to Webhook: {webhook_type}: {webhook_url}: Will run after {time} UTC")
            return

        try:
            sql_df = pandas.read_sql_query(
                query, conn).replace({nan: None})

            # Split data into smaller bundles based on the "NUM_OF_PAYLOADS" webhook YAML config
            list_of_dfs = (df for df in array_split(sql_df, num_of_payloads))

            for df in list_of_dfs:
                requests.post(
                    url=webhook_url,
                    data=df.to_json(orient='table', index=False),
                    headers={'Content-Type': 'application/json'}
                )

            logger.info(
                f"Data Sent to Webhook : {webhook_type}: {webhook_url}: {num_of_payloads} payloads")

        except Exception as e:
            logger.error(f"Webhook Error: {webhook_type}: {webhook_url}: {e}")
