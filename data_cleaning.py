from data_extraction import *
from dateutil.parser import parse

class DataCleaning(DataExcractor):


    '''
    method clean_user is used to clean data value such as creating correct datatype,
    removing Null value, removing duplicate value and adjusting the correct data_type.
    '''
    def clean_user_data(self):
        date_format = "mixed"
        df_user = self.read_rds_table('legacy_users')
        
        df_user.drop_duplicates(inplace= True)
        df_user.dropna(inplace= True)
        print(df_user.loc[752])
        df_user.drop(752, axis=0, inplace= True)
        print(df_user.loc[864])

        # df_user['date_of_birth'] = df_user['date_of_birth'].apply(parse)
        # df_user['date_of_birth'] = pd.to_datetime(df_user['date_of_birth'], infer_datetime_format=True, errors='coerce')

        # df_user['join_date'] = df_user['join_date'].apply(parse)
        # df_user['join_date'] = pd.to_datetime(df_user['join_date'], infer_datetime_format=True, errors='coerce')
        
        df_user.date_of_birth = pd.to_datetime(df_user.date_of_birth, format= date_format)
        df_user.join_date = pd.to_datetime(df_user.join_date, format= date_format)
        print(df_user.info())
        print(df_user["country"])
        print(df_user.dtypes)
    
    def clean_card_data(self):
        pass


    def called_clean_store_data(self):
        pass