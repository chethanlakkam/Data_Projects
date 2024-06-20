import snowflake.connector as sf
import os
import sys
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


class SnowflakeExtract:
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
            logging.info("Connected to Snowflake successfully")
        except:
            logging.info("Failed to connect to Snowflake")

    def execute_ddl_files(self):
        try:
            self.connect_to_snowflake()
            if self.conn is None:
                logging.info("Failed to connect to Snowflake. Exiting")
                return

            sql_file = sys.argv[1]
            table_name = os.path.splitext(sql_file)[0]
            file_path = os.path.join(self.folder_path, sql_file)
            try:
                with open(file_path, 'r') as file:
                    dml_command = file.read()
                    logging.info(f"Reading the data from {sql_file}")
                try:
                    with self.conn.cursor() as cs:
                        cs.execute(f"Truncate EMP_PROC.{table_name}")
                        logging.info(f"All the previous data from the table {table_name} is cleared")
                        cs.execute(dml_command)
                        logging.info(f"The query is processed successfully and the data is being inserted into {table_name}")
                except Exception as e:
                    logging.info("Failed to run the sql query")
            except Exception as e:
                logging.info(f"Unable to open the file {sql_file}")

            file_name_with_ext = os.path.basename(file_path)
            file_name = os.path.splitext(file_name_with_ext)[0]
            output_file_loc="/Users/chethankumarreddylakkam/Desktop/Data_Engineer/Project/extracts"
            #print(file_name)
            try:
                with self.conn.cursor() as cs:
                    query = f"SELECT * FROM EMP_PROC.{file_name}"
                    cs.execute(query)
                    results = cs.fetchall()
                    column_names = [desc[0] for desc in cs.description]
                    output_file_path = f'{file_name}_{datetime.now()}.txt'
                    output_loc=os.path.join(output_file_loc, output_file_path)
                    with open(output_loc, 'w') as f:
                        logging.info(f"Opening the file {output_file_path} to insert the data from table {table_name}")
                        f.write('|'.join(column_names) + '\n')
                        for row in results:
                            f.write('|'.join(str(cell) for cell in row) + '\n')
                    logging.info(f"The data is successfully inserted into file {output_file_path}")
                    #print(f"Output written to {output_loc}")
            except Exception as e:
                #print(f"Failed to execute query or write output: {e}")
                logging.inof("Failed to execute query or write output")
        finally:
            if self.conn:
                self.conn.close()
                #print("Connection closed.")
                logging.info("Connection closed successfully")

# Example usage
folder_path = '/Users/chethankumarreddylakkam/Desktop/Data_Engineer/Project/dml'
extractor = SnowflakeExtract(
    user = config.User_Details['user'],
    password = config.User_Details['password'],
    account=config.User_Details['account'],
    database=config.User_Details['database'],
    folder_path=folder_path
)
extractor.execute_ddl_files()
