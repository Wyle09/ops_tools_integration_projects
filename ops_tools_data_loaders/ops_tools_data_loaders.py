import asyncio
import os
import sys

from integrations.data import freshdesk, salesforce
from integrations.utils.database import (mysql_db_connections, run_sql_files,
                                         send_data_to_webhook)
from integrations.utils.misc import create_project_directories, yaml_config
from integrations.utils.project_logger import logger


async def main() -> None:
    logger.info('<<BEGIN>>')

    # ------ Config ----------
    config = yaml_config(
        yaml_file_path=os.path.join(sys.path[0], "data_loaders_config.yaml"))

    project_dirs = create_project_directories(
        project_dirs=config['PROJECT_FOLDERS'])

    mysql_db_conns = mysql_db_connections(config=config['MYSQL_DB'])

    # ------ Extract & Load ----------
    integration_list = [
        freshdesk.run_freshdesk_integration(
            config=config['FRESHDESK_API'],
            db_conn=mysql_db_conns['customer_success_freshdesk'],
            archive_folder_path=project_dirs['DATA_FD_ARCHIVE_FOLDER_PATH']
        ),
        salesforce.run_salesforce_integration(
            config=config['SALESFORCE_API'],
            db_conn=mysql_db_conns['customer_success_salesforce'],
            archive_folder_path=project_dirs['DATA_SF_ARCHIVE_FOLDER_PATH']
        )
    ]

    await asyncio.gather(*integration_list)

    logger.info('Data extraction & loading complete')

    # ------ Transform ----------
    run_sql_files(config['SQL_QUERIES'], connection=mysql_db_conns)

    logger.info('Data transformation complete')

    # ------ Send Data ----------
    send_data_to_webhook(
        config=config['WEBHOOKS'], connection=mysql_db_conns, env=config['ENV'])

    logger.info('Sending data complete')

    logger.info('<<END>>')


if __name__ == '__main__':
   asyncio.run(main())
