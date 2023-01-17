import asyncio
import json
import os
import re
from typing import Any

import requests

from integrations.utils.file_management import (create_json_file,
                                                delete_old_files,
                                                load_json_files, move_files)
from integrations.utils.misc import utc_date
from integrations.utils.project_logger import logger


def get_freshtdesk_data(api_key: str, pwd: str, endpoint_info: dict, updated_since_utc: str = None) -> dict[str, Any]:
    '''
    Retrieves data from Freshdesk API based for the given endpoint.

    :param: api_key
    :param: pwd
    :param: endpoint_info: values from yaml config FRESHDESK_API.ENDPOINTS
    :return: Dict {"STATUS": "error" | "pending" | "success", "TYPE": values from yaml config
    FRESHDESK_API.ENDPOINTS.TYPE, "DATA": API response or default message.}
    '''
    data_files = [f for f in os.listdir(f"{os.getcwd()}/data")
                  if f.startswith(endpoint_info['TYPE']) & f.endswith('.json')]

    # Check if there are any files pending for import
    if data_files:
        data = {
            'STATUS': 'pending',
            'TYPE': endpoint_info['TYPE'],
            'URL': endpoint_info['URL'],
            'DATA': 'Existing file pending for import, Check data folder'
        }
        logger.info(data)
        return(data)

    # Check if endpoint url requires a time filter.
    if 'updated_since=' in endpoint_info['URL']:
        endpoint_info['URL'] = f"{endpoint_info['URL']}{updated_since_utc}"

    response = requests.get(url=endpoint_info['URL'], auth=(api_key, pwd))
    print(response)

    try:
        # If status is sucessful and pagination is not needed.
        if response.status_code == 200 and 'Link' not in response.headers:
            logger.info('Requests processed successfully (1 page)')
            data = {
                'STATUS': 'sucess',
                'TYPE': endpoint_info['TYPE'],
                'URL': endpoint_info['URL'],
                'DATA': json.loads(response.content)
            }
            return data
        # If status is sucessful and pagination is needed.
        elif response.status_code == 200 and 'Link' in response.headers:
            logger.info('Requests processed successfully(Multiple pages)')
            response_content = []

            # Store the values from the first link
            response_content.extend(json.loads(response.content))
            next_url = re.findall("<(.+?)>", response.headers['Link'])[0]

            while next_url:
                response = requests.get(url=next_url, auth=(api_key, pwd))
                response_content.extend(json.loads(response.content))

                if 'Link' in response.headers:
                    next_url = re.findall(
                        "<(.+?)>", response.headers['Link'])[0]
                else:
                    logger.info(f"Last page url = {next_url}")
                    next_url = None

            data = {
                'STATUS': 'sucess',
                'TYPE': endpoint_info['TYPE'],
                'URL': endpoint_info['URL'],
                'DATA': response_content
            }
            return data
        else:
            data = {
                'STATUS': 'error',
                'TYPE': endpoint_info['TYPE'],
                'URL': endpoint_info['URL'],
                'DATA': response
            }
            logger.error(data)
            return data

    except Exception as e:
        logger.error(f"{e}: {data}")
        return data


async def run_freshdesk_integration(config: dict, db_conn: object, archive_folder_path: str) -> None:
    '''
    Executes the following sequences for each endpoint type specified in the yaml config. Get data
    >> Create JSON file >> Import JSON file >> Archive File >> Delete Old Files.

    :param: config: yaml config "SALESFORCE_API" dict
    :param: db_conn: Connection object for customer_success_salesforce schema
    :param: archive_folder_path: file archive folder path.
    :return: None
    '''
    if not config['ENDPOINTS']:
        logger.info('Missing endpoints - Skip Integration')
        return

    fd_endpoints = (
        endpoint for endpoint in config['ENDPOINTS'])

    # UTC Timestamp for time range filter
    fd_updated_since_utc = utc_date(
        interval_num=int(config['INTERVAL_DAYS']))['INTERVAL_DATE']

    try:
        for endpoint in fd_endpoints:
            fd_data = get_freshtdesk_data(
                api_key=config['API_KEY'],
                pwd=config['PWD'],
                endpoint_info=endpoint,
                updated_since_utc=fd_updated_since_utc
            )

            # Loading files to Staging tables
            create_json_file(fd_data, utc_date()['CURRENT_DATE_UTC'])
            imported_files = load_json_files(
                endpoint_type=endpoint['TYPE'],
                connection=db_conn
            )

            move_files(imported_files=imported_files,
                       archive_path=archive_folder_path)
    except Exception as e:
        logger.error(f"Integration error: {e}: {fd_data}")
        return

    delete_old_files(archive_directories=[archive_folder_path])

    logger.info('Freshdesk extraction & loading complete')
