import tabula
from database_utils import *
from sqlalchemy import *
import pandas as pd

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

class DataExcractor(DatabaseConnector):
    
    
    '''
    The def init uses the super to inherit all the Database connector
    which will allow the DataExctractor class ot call the specific methods
    '''
    # def __init__(self, table_ls, engine):
    #     self.init_db_engine = engine
    #     self.list_db_tables = table_ls
    
    def list_db_tables(self):
        
        engine = self.init_db_engine()
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        table_ls = inspect(engine)
        
        return table_ls.get_table_names()
    
    
    '''
    Reads the data from the table from the list_db_tables.
    '''
    def read_rds_table(self, table_name):
        
        table_ls = self.list_db_tables()
        self.table_name = table_name
        engine = self.init_db_engine()
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()


        if self.table_name in table_ls:
            df_user = pd.read_sql_table(f'{self.table_name}', engine)


        return df_user
        
    
    def retrieve_pdf_data(self):
        tabula.read_pdf(pdf_path)
            
    
    def list_number_of_stores(self):
        pass


    def retrieve_stores_data(self):
        pass
    