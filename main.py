from database_utils import DatabaseConnector
from data_extraction import DataExcractor
from data_cleaning import DataCleaning

'''

This main used to import all the three classes for data cleaning,
data extraction and data base connector.
'''

dcon= DatabaseConnector()
dx = DataExcractor()
dcl = DataCleaning()

store_details =  "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
number_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
aws_json = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
amz_s3 = "s3://data-handling-public/products.csv"

'''
The variable above contains the initialisation of three data classes,
url for api link and s3 links resources to be used inside a parameter.
'''
def extract_clean_user_data():

    '''
    This function creates an engine to extract the user_data from a sql table
    with cleaned up dataframe. Once cleaned, it is uploaded to the sql server 
    costumer_data.
    '''
    engine = dcon.init_db_engine()
    table_list = dx.list_db_tables(engine)
    df_user = dx.read_rds_table(engine, table_list, 'legacy_users')
    upload_df = dcl.clean_user_data(df_user)
    dcon.upload_to_db('dim_users', upload_df)

def extract_clean_card():

    '''
    This function calls in to take a specific data from a pdf format, which is then used
    to conver into a dataframe to be cleaned up.
    The cleaned up data is downloaded directly to the sql customer_data server
    '''
    df_pdf = dx.retrieve_pdf_data()
    upload_card_df = dcl.clean_card_data(df_pdf)
    dcon.upload_to_db('dim_card_details', upload_card_df)

def extract_clean_store():

    '''
    This function uses the api request to retrieve the number of stores available and is used to feedback
    to the store_details with a required x-api-key.
    The api data are converted and cleaned in panda dataframe which is followed by uploading to the sql customer_data
    server.
    '''
    dict_header= dcon.read_db_creds()
    df_store = dx.retrieve_stores_data(store_details, number_stores, dict_header)
    upload_df_store = dcl.called_clean_store_data(df_store)
    dcon.upload_to_db("dim_store_details", upload_df_store)

def extract_clean_product_details():

    '''
    This function takes in an s3 bucket link to gain access to the bucket list name and key to access the csv file
    for product_details. There where two clean methods required to clean the product_details weight into correct metric
    weight (kg) as well as standard cleaning. 
    '''
    aws_ex = dx.extract_from_s3(amz_s3)
    aws_clean_df = dcl.clean_products_data(aws_ex)
    dcon.upload_to_db("dim_products",aws_clean_df)

def extract_clean_order_table():

    '''
    This is similar to the previous excercise for function for creating an engine to retrieve ordes_table
    information. The orders_table is cleaned and is uploaded to the customer_data server. 
    '''
    engine = dcon.init_db_engine()
    table_list = dx.list_db_tables(engine)
    df_order = dx.read_rds_table(engine, table_list, 'orders_table')
    clean_df_order = dcl.clean_orders_data(df_order)
    dcon.upload_to_db("orders_table", clean_df_order)

def exctract_clean_date():

    '''
    This function uses a http link for s3 buckets which could have been extracted different ways,
    instead using the boto3 to extract and clean the json file before uploading to the customer_data server.
    '''
    df_json = dx.extract_from_http_s3(aws_json)
    clean_df_json = dcl.clean_json(df_json)
    dcon.upload_to_db("dim_date_times", clean_df_json)


if __name__ == "__main__":

    '''
    Using the if__name__ == "__main__" to run the code if running directly inside the main.py.
    Each task does different cleaning methods applied for api, sql or s3 source data to clean each data.
    '''
    extract_clean_user_data()
    extract_clean_card()
    extract_clean_store()
    extract_clean_product_details()
    extract_clean_order_table()
    exctract_clean_date()
    
 
    



