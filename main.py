from database_utils import DatabaseConnector
from data_extraction import DataExcractor
from data_cleaning import DataCleaning


dcon= DatabaseConnector()
dx = DataExcractor()
dcl = DataCleaning()

'''
This main.py is used to test each methods from different class.
'''


if __name__ == "__main__":
    engine = dcon.init_db_engine()
    table_list = dx.list_db_tables(engine)
    df_user = dx.read_rds_table(engine, table_list, 'legacy_users')
    upload_df = dcl.clean_user_data(df_user)
    print(dcon.read_db_creds())
    print(dcon.init_db_engine())
    print(table_list)
    print(df_user)
    dcl.clean_user_data(df_user)
    dcon.upload_to_db(upload_df, 'dim_users')
