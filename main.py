from database_utils import DatabaseConnector
from data_extraction import DataExcractor
from data_cleaning import DataCleaning


dcon= DatabaseConnector()
dx = DataExcractor()
dcl = DataCleaning()

store_details =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
number_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
'''
This main.py is used to test each methods from different class.
'''


if __name__ == "__main__":
    engine = dcon.init_db_engine()
    table_list = dx.list_db_tables(engine)
    df_user = dx.read_rds_table(engine, table_list, 'legacy_users')
    df_pdf = dx.retrieve_pdf_data()
    dict_header= dcon.read_db_creds()
    # print(dcon.init_db_engine())
    # print(table_list)
    # print(df_user)
    upload_df = dcl.clean_user_data(df_user)
    dcon.upload_to_db('dim_users', upload_df)
    # print(df_pdf)
    upload_card_df = dcl.clean_card_data(df_pdf)
    dcon.upload_to_db('dim_card_details', upload_card_df)
