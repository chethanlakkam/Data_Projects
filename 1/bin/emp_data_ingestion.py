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

class DataIngest:
    def __init__(self, user, password, account, database, folder_path):
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.folder_path = folder_path
        self.conn = None
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
        finally:
            self.cs= self.conn.cursor()
    def DML_Command(self):
        try:
            self.connect_to_snowflake()
        except:
            loggging.info("Cannot connect to snowflake")
            
        text_files = [f for f in os.listdir(self.folder_path) if f.endswith('.txt')]
        for text_file in text_files:
            table_name=os.path.splitext(text_file)[0]
            file_path=os.path.join(self.folder_path,text_file)
            try:
                self.cs.execute(f"Truncate EMP_RAW.{table_name}")
                logging.info(f"Clearing all the data from the {table_name} table so that data in a table cannot be inserted again and again")
                logging.info(f"Reading the data from the {text_file} into dataframe")
                df=pd.read_csv(file_path,header=None, sep = '|')
                
                df.replace({np.nan: None},inplace=True)
                
                
                logging.info("Loading the data into the table using insert command")
                try:
                    for _, row in df.iterrows():
                        placeholders = ', '.join(['%s']* len(row))
                        insert_query = f"INSERT INTO EMP_RAW.{table_name} VALUES({placeholders})"
                        
                        self.cs.execute(insert_query , tuple(row))
                    self.conn.commit()
                    logging.info(f"Data is loaded into  {table_name} table")
                except:
                    logging.info(f"unable to load data into table {table_name}")
            except:
                logging.info(f"Failed to open file {text_file}")
        self.conn.close()
        logging.info("Connection closed")

folder_path = '/Users/chethankumarreddylakkam/Desktop/Data_Engineer/Project/inbound_data'

executor = DataIngest(
    user = config.User_Details['user'],
    password = config.User_Details['password'],
    account=config.User_Details['account'],
    database=config.User_Details['database'],
    folder_path=folder_path
)
executor.DML_Command()