import tabula
# from database_utils import DatabaseConnector
from sqlalchemy import *
import pandas as pd

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
        df_pdf = tabula.read_pdf(pdf_path)
        df_pdf = pd.DataFrame(df_pdf)

        return df_pdf    
    
    def list_number_of_stores(self):
        pass


    def retrieve_stores_data(self):
        pass
    