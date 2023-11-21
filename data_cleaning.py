# from data_extraction import DataExcractor
from dateutil.parser import parse
import pandas as pd

class DataCleaning():


    '''
    method clean_user is used to clean data value such as creating correct datatype,
    removing Null value, removing duplicate value and adjusting the correct data_type.
    '''
    def clean_user_data(self, df_user):
        date_format = "mixed"
        
        # Dex = DataExcractor()
        # df_user = self.read_rds_table('legacy_users')
        
        df_user.drop_duplicates(inplace= True)

        # df_user['date_of_birth'] = df_user['date_of_birth'].apply(parse)
        # df_user['date_of_birth'] = pd.to_datetime(df_user['date_of_birth'], infer_datetime_format=True, errors='coerce')

        # df_user['join_date'] = df_user['join_date'].apply(parse)
        # df_user['join_date'] = pd.to_datetime(df_user['join_date'], infer_datetime_format=True, errors='coerce')
        
        df_user.date_of_birth = pd.to_datetime(df_user.date_of_birth, format= date_format, errors= 'coerce')
        df_user.join_date = pd.to_datetime(df_user.join_date, format= date_format, errors='coerce')

        df_user.dropna(inplace= True)

        # print(df_user.info())
        # print(df_user["date_of_birth"])
        # print(df_user.dtypes)

        return df_user 
       
    def clean_card_data(self, card_df):
        

        '''
        method used to clean the card_data list from a pdf file containing user
        information
        '''
        # card_df = self.retrieve_pdf_data
        
        card_df.drop_duplicates(inplace= True)
        
        card_df.expiry_date = pd.to_datetime(card_df.expiry_date, format='%m%y', errors='coerce')
        card_df.date_time_payment = pd.to_datetime(card_df.date_time_payment, format='mixed', errors='coerce')
        
        card_df.dropna(inplace= True)

        return card_df


    def called_clean_store_data(self):
        pass