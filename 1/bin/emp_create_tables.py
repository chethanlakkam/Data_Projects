import snowflake.connector as sf
import pandas as pd
import numpy as np
import os
import logging
import config
from datetime import datetime

LOG_FILE=f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
logs_path=os.path.join(os.getcwd(),"/Users/chethankumarreddylakkam/Desktop/Data_Engineer/Project/logs",LOG_FILE)
os.makedirs(logs_path,exist_ok=True)

LOG_FILE_PATH=os.path.join(logs_path,LOG_FILE)

logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    )

class SnowflakeDDLExecutor:
    def __init__(self, user, password, account, database, folder_path):
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.folder_path = folder_path
        self.conn = None
        self.executed_files = []
    def connect_to_snowflake(self):
        try:
            self.conn = sf.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                database=self.database
                )
            logging.info("Connected to snowflake successfully")
        except:
            logging.error("Failed to connect to snowflake")
    def DDL_Command(self):
        try:
            self.connect_to_snowflake()
        except:
            loggging.info("Cannot connect to snowflake")
        ddl_files = [f for f in os.listdir(self.folder_path) if f.endswith('.sql')]
        for ddl_file in ddl_files:
            if ddl_file not in self.executed_files:
                file_path = os.path.join(self.folder_path, ddl_file)
                try:
                    with open(file_path,'r') as file:
                        ddl_command = file.read()
                        logging.info(f"Executing DDL commands from file: {ddl_file}")
                        try:
                            with self.conn.cursor() as cs:
                                cs.execute(ddl_command)
                                logging.info("Table created successfully")
                                self.executed_files.append(ddl_file)
                        except:
                            logging.info("Failed to create table ")
                except:
                    logging.info(f"Unable to open file {ddl_file}")
            else:
                logging.info("All the tables are already created")
        self.conn.close()
        logging.info("Connection Closed")

folder_path = '/Users/chethankumarreddylakkam/Desktop/Data_Engineer/Project/ddl'

executor = SnowflakeDDLExecutor(
    user = config.User_Details['user'],
    password = config.User_Details['password'],
    account=config.User_Details['account'],
    database=config.User_Details['database'],
    folder_path=folder_path
    )
executor.DDL_Command()
