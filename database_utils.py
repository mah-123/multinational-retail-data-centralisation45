import yaml
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
# from data_cleaning import DataCleaning

class DatabaseConnector():
    
    '''
    Creating a dictionary to contain the information from the db_creds.yaml
    file for creating a db create engine.Note: might need to make it private or protected
    from user entering in.
    '''
    def read_db_creds(self):
        
        with open("db_creds.yaml", 'r') as file:
            dict = yaml.safe_load(file)
        return dict


    '''
    Cretaing a create enginee which will be retrieved from the read_db_creds.
    Each value had been assigned to the database
    '''
    def init_db_engine(self):

        db_val = self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = db_val['RDS_HOST']
        USER = db_val['RDS_USER']
        PASSWORD = db_val['RDS_PASSWORD']
        DATABASE = db_val['RDS_DATABASE']
        PORT = db_val['RDS_PORT']
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def upload_to_db(self, table_name, upload_df):
        
        
        '''
        Uses the Datacleaning class filtered dataframe from clean_user_data() methods to take
        in the clean filtered. This then sent to the sales_data database to be query from SQL.
        '''
        # Dcl = DataCleaning()
        # upload_df = Dcl.clean_user_data()
        self.table_name = table_name

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST ='localhost' 
        USER = 'postgres'
        PASSWORD = 'Mahdi_123!'
        DATABASE = 'sales_data'
        PORT = 5423
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return upload_df.to_sql(f'{table_name}', engine, if_exists = 'replace')

