import tabula
from database_utils import *

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

class DataExcractor(DatabaseConnector):
    
    
    def __init__(self):
        pass

    
    def list_db_tables(self):
        pass

    
    def read_rds_table(self):
        pass

    
    def retrieve_pdf_data(self):
        tabula.read_pdf(pdf_path)
            
    
    def list_number_of_stores(self):
        pass


    def retrieve_stores_data(self):
        pass
    