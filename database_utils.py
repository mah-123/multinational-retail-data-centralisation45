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

        '''
        Method used to read the creadential file from a yaml file containing
        important information.

            returns:
                dict: dictionary of the credential details.
        '''
        with open("db_creds.yaml", 'r') as file:
            dict = yaml.safe_load(file)
        return dict

    def read_db_creds_2(self):
        
        '''
        Similar to the previous method used to read the creadential file from a yaml file containing
        important information.

            returns:
                dict: dictionary of the sales credential details.
        '''
        with open("db_sales_creds.yaml", 'r') as file:
            dict = yaml.safe_load(file)
        return dict

    def init_db_engine(self):

        '''
        
        Method used to create an enginee which will be retrieved from the read_db_creds.
        Each value had been assigned to the database

            return:
                engine: use for creating a connection point to sql.
        '''
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
        Method to allow uploading from Datacleaning class filtered dataframe to sql query customer_data server.

            args:
                table_name: parameter to name the specific table name on sql.
                upload_df: parameter used to take in the dataframe and upload to the sql server.
            returns:
                print statement: suggest if it had been successful for upload.
        '''
    
        db_val = self.read_db_creds_2()

        DATABASE_TYPE = db_val['DATABASE_TYPE']
        DBAPI = db_val['DBAPI']
        HOST = db_val['HOST']
        USER = db_val['USER']
        PASSWORD = db_val['PASSWORD']
        DATABASE = db_val['DATABASE']
        PORT = db_val['PORT']
        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        
        upload_df.to_sql(f'{table_name}', engine, if_exists = 'replace')

        print("upload complete")

