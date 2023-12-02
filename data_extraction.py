import tabula
# from database_utils import DatabaseConnector
from sqlalchemy import *
import pandas as pd
import requests
import json
import boto3

pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"


class DataExcractor():
    
    
    '''
    The def init uses the super to inherit all the Database connector
    which will allow the DataExctractor class ot call the specific methods
    '''
    
    def list_db_tables(self, engine):
        
        '''
        This methods uses the parameter to create an engine to access
        sql file.
            
            args:
                engine: creates a connection to the sql server.
            return: 
                table list
        '''
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        table_ls = inspect(engine)
        
        return table_ls.get_table_names()
    
    
    def read_rds_table(self, engine, table_ls, table_name):
        
        '''
        Method reads the data from the table from the list_db_tables.
            
            args:
                engine: creates a connection point to sql server.
                table_ls: obtains a list of tables from the list_db_tables method
                table_name: input the str name to find if it contains in the table_ls
            returns:
                dataframe for the specific table_name

        '''
        table_ls = self.list_db_tables(engine)
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()


        if table_name in table_ls:
            df_table = pd.read_sql_table(f'{table_name}', engine)


        return df_table
        
    
    def retrieve_pdf_data(self):
        
        '''
        Methods uses to extract specificly a pdf file using the tabula import.

            args:
                df_pdf: using the pdf link to read all the page details.
                df_concar: variable to create a dataframe from the extracted pdf file.
            returns:
                The dataframe from the pdf file.
        '''
        df_pdf = tabula.read_pdf(pdf_path, pages= 'all', stream= False)
        df_concat = pd.concat(df_pdf)
        return df_concat   
    
    def list_number_of_stores(self, store_endpoint, dict_header):

        '''
        Methods list_of_numbers takes in the endpoint and dictionary header paramater
        to gain some information for the number of stores.
            args:
                store_endpoint: api link to access the number of store available. 
                dict_header: information used to access the x-api-key for the api request.
            returns:
                data: list for the number of store. 
        '''
        response = requests.get(store_endpoint, headers=dict_header)
        
        if response.status_code == 200:
            data = response.json()

        return data


    def retrieve_stores_data(self, store_endpoint, number_stores_endpoint, dict_header):
        
        '''
        Methods uses the to extract a specific api to form a dataframe for store_data.
            
            args:
                store_endpoint: api link to access each stores detail.
                store_endpoint: api link to access the number of store available.
                dict_header: information used to access the x-api-key for the api request.
            returns:
                df_store: dataframe which can be used to clean the store_details.
        '''
        headers = {"x-api-key": dict_header['X-API-KEY']}
        num_stores = self.list_number_of_stores(number_stores_endpoint, headers)

        data = []
        
        for idx in range(num_stores['number_stores']):

            response = requests.get(store_endpoint.format(store_number=idx), headers=headers)

            repos = response.json()
            
            data.append(repos)
        
        df_store = pd.DataFrame(data)

        return df_store
    
    
    def extract_from_s3(self, amz_s3):
        
        '''
        Method uses the amazon s3 bucket address to obtain the dataframe for product_details.

            args:
                amz_s3: s3 address link parameter input.
            returns:
                s3_df: dataframe.
        '''
        bucket, key = amz_s3.replace("s3://", "").split("/")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket= bucket, Key= key)
        # s3.download_file(bucket, key, "C:/Users/Student/database-project/")
        s3_df = pd.read_csv(response['Body'])
        
        return s3_df
    
    def extract_from_http_s3(self, http_s3):
        
        '''
        Method uses the http link that contains the s3 json file.
            
            args:
                http_s3: parameter containing the link for the s3 bucket list.
            returns:
                s3_df: dataframe from a json file.
        '''
        http_s3 = http_s3.replace("https://", "")
        ls_s3 = []
            
        tmp, key = http_s3.replace("https://", "").split("/")
        ls_s3 = tmp.split(".")
        bucket = ls_s3[0]
        s3 = boto3.client('s3')

        response = s3.get_object(Bucket= bucket, Key= key)
        s3_df = pd.read_json(response['Body'])
        
        return s3_df

    