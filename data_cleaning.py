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
        
        card_df.drop_duplicates()
        #Adjust the format for expiry date.
        card_df['expiry_date'] = pd.to_datetime(card_df.expiry_date, format='%m/%y', errors='coerce')
        card_df['date_time_payment'] = pd.to_datetime(card_df.date_payment_confirmed, format='"%Y%m%d"', errors='coerce')
        
        card_df.dropna()
        print(type(card_df))

        return card_df


    def called_clean_store_data(self, store_df):
        
        store_df.drop_duplicates()
        # print(store_df.info())
        store_df['address'] = store_df['address'].str.replace("\n", ",")
        store_df['longitude'] = pd.to_numeric(store_df.longitude, errors='coerce')
        del store_df['lat']
        # store_df.drop(columns='lat')
        # print(store_df['lat'].unique())
        # print(store_df['locality'].unique())
        # print(store_df['store_code'].unique())
        store_df['staff_numbers'] = pd.to_numeric(store_df.staff_numbers, errors='coerce')
        # print(store_df['store_type'].unique())
        # print(store_df['locality'].unique())
        # print(store_df['country_code'].unique())
        store_df['latitude'] = pd.to_numeric(store_df.latitude, errors='coerce')
        
        map_dict = {'eeEurope': 'Europe',
                    'eeAmerica': 'America'
                    }
        store_df['continent'].replace(map_dict, inplace= True)
        
        store_df.dropna(subset=['latitude'], inplace=True)

        return store_df
    '''
    converts all the weight units to kg where each value are in oz, ml, g and mutiple grams.
    Any random value needs to removed and turned to a float value.
    '''
    def convert_product_weights(self, aws_df):
        
        multi_gram = ['x','g']

        for idx in range(len(aws_df['weight'])):
            
            if 'x' in aws_df['weight'][idx]:
                val_1, val_2 = aws_df['weight'][idx].str.replace(multi_gram, '').split()
                aws_df['weight'][idx] = (int(val_1) * int(val_2))/1000
            
            elif 'kg' in aws_df['weight'][idx]:
                aws_df['weight'][idx] = aws_df['weight'][idx].str.replace('kg', '')
                

            elif 'g' in aws_df['weight'][idx]:
                aws_df['weight'][idx] = aws_df['weight'][idx].str.replace('g', '')
                aws_df['weight'][idx] = (int(aws_df['weight'][idx]))/1000
                
            
            elif 'oz' in aws_df['weight'][idx]:
                aws_df['weight'][idx] = aws_df['weight'][idx].str.replace('oz', '')
                aws_df['weight'][idx] = int(aws_df['weight'][idx])/ 35.274
                
            
            elif 'ml' in aws_df['weight'][idx]:
                aws_df['weight'][idx] = aws_df['weight'][idx].str.replace('ml', '')
                aws_df['weight'][idx] = (int(aws_df['weight'][idx]))/1000
                

            else:
                del aws_df['weight'][idx]
                #might be left out at the end of conversion for all columns
        
        aws_df['weight'] = pd.to_numeric(aws_df['weight'], errors='coerce')
        
        return aws_df
            