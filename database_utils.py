import yaml
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

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

    def upload_to_db():
        # dim_card_details
        pass