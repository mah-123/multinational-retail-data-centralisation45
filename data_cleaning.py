# from data_extraction import DataExcractor
from dateutil.parser import parse
import pandas as pd

class DataCleaning():


    '''
    method clean_user is used to clean data value such as creating correct datatype,
    removing Null value, removing duplicate value and adjusting the correct data_type.
    '''
    def clean_user_data(self, df_user):
        # regex_GB = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        # regex_US = '^[2-9]\d{2}-\d{3}-\d{4}$'
        # regex_DE = '^((00|\+)49)?(0?[2-9][0-9]{1,})$'
        
        date_format = "mixed"
        map_dict = {'GGB': 'GB'}
        df_user['country_code'].replace(map_dict, inplace= True)
        df_user['address'] = df_user['address'].str.replace("\n", ",")
        # df_user['address'] = df_user['address'].apply(lambda x:x.replace("\n", ','))
        # print(df_user['date_of_birth'].unique())
        # print(df_user['join_date'].unique())
        # print(df_user['country'].unique())


        # print(df_user['phone_number'].unique())
        # print(df_user['phone_number'].value_counts())
        
        
        df_user.drop_duplicates()

        # df_user['date_of_birth'] = df_user['date_of_birth'].apply(parse)
        # df_user['date_of_birth'] = pd.to_datetime(df_user['date_of_birth'], infer_datetime_format=True, errors='coerce')

        # df_user['join_date'] = df_user['join_date'].apply(parse)
        # df_user['join_date'] = pd.to_datetime(df_user['join_date'], infer_datetime_format=True, errors='coerce')
        
        df_user['date_of_birth'] = pd.to_datetime(df_user.date_of_birth, format= date_format, errors= 'coerce')
        df_user['join_date'] = pd.to_datetime(df_user.join_date, format= date_format, errors='coerce')

        df_user.dropna()

        # print(df_user.info())
        # print(df_user["date_of_birth"])
        # print(df_user.dtypes)

        return df_user 
    '''
    This method will clean the user information for card reading details.
    '''
    def clean_card_data(self, card_df):
        

        '''
        method used to clean the card_data list from a pdf file containing user
        information
        '''
        print(card_df)
        card_df.drop_duplicates()
        #Adjust the format for expiry date.
        card_df['expiry_date'] = pd.to_datetime(card_df.expiry_date, format='%m/%y', errors='coerce')
        card_df['date_time_payment'] = pd.to_datetime(card_df.date_payment_confirmed, format='"%Y%m%d"', errors='coerce')
        
        card_df.dropna()
        print(type(card_df))

        return card_df


    def called_clean_store_data(self, store_df):
        pass