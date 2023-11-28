from database_utils import DatabaseConnector
from data_extraction import DataExcractor
from data_cleaning import DataCleaning


dcon= DatabaseConnector()
dx = DataExcractor()
dcl = DataCleaning()

store_details =  "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
number_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
amz_s3 = "s3://data-handling-public/products.csv"
'''
This main.py is used to test each methods from different class.
'''


if __name__ == "__main__":
    engine = dcon.init_db_engine()
    table_list = dx.list_db_tables(engine)
    df_user = dx.read_rds_table(engine, table_list, 'legacy_users')
    df_pdf = dx.retrieve_pdf_data()
    dict_header= dcon.read_db_creds()
    aws_ex = dx.extract_from_s3(amz_s3)
    weight_df = dcl.convert_product_weights(aws_ex)
    # print(dcon.init_db_engine())
    # print(table_list)
    # print(df_user)
    # upload_df = dcl.clean_user_data(df_user)
    # dcon.upload_to_db('dim_users', upload_df)
    # # print(df_pdf)
    # upload_card_df = dcl.clean_card_data(df_pdf)
    # dcon.upload_to_db('dim_card_details', upload_card_df)
    #reading the number of stores from the endpoint
    # df_store = dx.retrieve_stores_data(store_details, number_stores, dict_header)
    # upload_df_store = dcl.called_clean_store_data(df_store)
    # dcon.upload_to_db("dim_store_details", upload_df_store)
    print(aws_ex.info())
    print(aws_ex['weight'].unique())
    print(weight_df['weight'])


