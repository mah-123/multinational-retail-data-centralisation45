import tabula
# from database_utils import DatabaseConnector
from sqlalchemy import *
import pandas as pd
import requests

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"


class DataExcractor():
    
    
    '''
    The def init uses the super to inherit all the Database connector
    which will allow the DataExctractor class ot call the specific methods
    '''
    
    def list_db_tables(self, engine):
        
        # dc = DatabaseConnector()
        # engine = dc.init_db_engine()
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        table_ls = inspect(engine)
        
        return table_ls.get_table_names()
    
    
    '''
    Reads the data from the table from the list_db_tables.
    '''
    def read_rds_table(self, engine, table_ls, table_name):
        
        table_ls = self.list_db_tables(engine)
        self.table_name = table_name
        # engine = self.init_db_engine()
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()


        if self.table_name in table_ls:
            df_user = pd.read_sql_table(f'{self.table_name}', engine)


        return df_user
        
    
    def retrieve_pdf_data(self):
        df_pdf = tabula.read_pdf(pdf_path, pages= 'all', stream= False)
        df_concat = pd.concat(df_pdf)
       # df_pdf = pd.DataFrame(df_pdf)
        print(f'This after the data pool{type(df_concat)}')
        return df_concat   
    '''
    Methods list_of_numbers takes in the endpoint and dictionary header paramater
    to gain some information for the number of stores 
    '''
    def list_number_of_stores(self, store_endpoint, dict_header):
        self.store_endpoint = store_endpoint
        self.dict_header = dict_header
        
        headers = {'Authorisation': dict_header['X-API-KEY']}
        response = requests.get(store_endpoint, headers=headers)

        return response


    def retrieve_stores_data(self):
        pass
    