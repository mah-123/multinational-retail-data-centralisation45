# from data_extraction import DataExcractor
from dateutil.parser import parse
import pandas as pd
import numpy as np
import re

class DataCleaning():

    def clean_user_data(self, df_user):

        '''
        method clean_user is used to clean data value such as creating correct datatype,
        removing Null value, removing duplicate value and adjusting the correct data_type.

            args:
                df_user: parameter to take in a dataframe of the user information.
            returns:
                df_user: cleaned dataframe for df_user.
        '''
        date_format = "mixed"
        map_dict = {'GGB': 'GB'}
        df_user['country_code'].replace(map_dict, inplace= True)
        df_user['address'] = df_user['address'].str.replace("\n", ",")
        
        
        df_user.drop_duplicates()
        
        df_user['date_of_birth'] = pd.to_datetime(df_user.date_of_birth, format= date_format, errors= 'coerce')
        df_user['join_date'] = pd.to_datetime(df_user.join_date, format= date_format, errors='coerce')

        df_user.dropna(subset=['date_of_birth'], inplace=True)

        return df_user 
    '''
    This method will clean the user information for card reading details.
    '''
    def clean_card_data(self, card_df):
        

        '''
        method used to clean the card_data list from a pdf file containing user
        information

            args:
                card_df: parameter for taking in the dataframe for card details.
            returns:
                card_df: returns a cleaned dataframe for list of card details.
        '''
        
        card_df.drop_duplicates()
        #Adjust the format for expiry date.
        card_df['expiry_date'] = pd.to_datetime(card_df.expiry_date, format='%m/%y', errors='coerce')
        card_df['date_time_payment'] = pd.to_datetime(card_df.date_payment_confirmed, format='"%Y%m%d"', errors='coerce')
        
        card_df.dropna()

        return card_df


    def called_clean_store_data(self, store_df):
        
        '''
        Method for cleaning the dataframe of the store_details.

            args:
                store_df: parameter to clean the store details dataframe.
            returns:    
                store_df: returns cleaned up version of the store details dataframe.
        '''
        store_df.drop_duplicates()
        store_df['address'] = store_df['address'].str.replace("\n", ",")
        store_df['longitude'] = pd.to_numeric(store_df.longitude, errors='coerce')
        store_df['staff_numbers'] = pd.to_numeric(store_df.staff_numbers, errors='coerce')
        store_df['latitude'] = pd.to_numeric(store_df.latitude, errors='coerce')
        store_df['lat'] = pd.to_numeric(store_df.lat, errors='coerce')
        
        map_dict = {'eeEurope': 'Europe',
                    'eeAmerica': 'America'
                    }
        
        store_df['continent'].replace(map_dict, inplace= True)
        
        store_df.dropna(subset=['latitude'], inplace=True)

        return store_df
    

    @staticmethod
    def kg_converter(s):

        '''
        Statticmethod used for apply method to on the dataframe weight conversion. 
        This conversion is applied for anything containing the 'kg' weight.
            
            args:
                s: place holder to find any value containing kg.
            returns:
                s: returns a kg value without the 'kg' notation, else leaves the orignal value. 
        '''
        if "kg" in s:
            s = s.replace("kg", '')
            return s
        else:
            return s
    
    @staticmethod
    def multi_g_converter(s):

        
        '''
        Statticmethod used for apply method to on the dataframe weight conversion. 
        This conversion is applied for anything containing the 'x' or multiplying grams weight.
            
            args:
                s: place holder to find any value containing 'x'.
            returns:
                s: returns a calculated kg conversion value without the 'x' and 'g' notation, else leaves the orignal value. 
        '''
        if "x" in s:
            s = s.replace(" x ", ' ')
            num_1, num_2 = s.split(" ")[0], s.split(" ")[1][:-1]
            val = (float(num_1) * float(num_2)) / 1000
            return str(val)
        else:
            return s
        
    @staticmethod
    def ml_converter(s):

        
        '''
        Statticmethod used for apply method to on the dataframe weight conversion. 
        This conversion is applied for anything containing the 'ml' weight.
            
            args:
                s: place holder to find any value containing ml.
            returns:
                s: returns a calulated kg conversion value without the 'ml' notation, else leaves the orignal value. 
        '''
        if "ml" in s:
            s = s.replace("ml", ' ')
            val = float(s)/1000
            return str(val)
        else:
            return s

    @staticmethod
    def g_converter(s):

        '''
        Statticmethod used for apply method to on the dataframe weight conversion. 
        This conversion is applied for anything containing the 'g' weight.
            
            args:
                s: place holder to find any value containing g.
            returns:
                s: returns a calulated kg conversion value without the 'g' notation, else leaves the orignal value. 
        '''
        if "g" in s:
            s = s.replace("g", ' ')
            if s[-1] == ".":
                s = s[:-1]
            val = float(s)/1000
            return str(val)
        else:
            return s
    
    @staticmethod
    def oz_converter(s):

        '''
        Statticmethod used for apply method to on the dataframe weight conversion. 
        This conversion is applied for anything containing the 'oz' weight.
            
            args:
                s: place holder to find any value containing oz.
            returns:
                s: returns a calulated kg conversion value without the 'oz' notation, else leaves the orignal value. 
        '''
        if "oz" in s:
            s = s.replace("oz", ' ')
            val = round(float(s)/35.274, 2)
            return str(val)
        else:
            return s
    
    def convert_product_weights(self, aws_df):

        '''
        Method uses the statistic method to apply a conversion for the string value for each weight.

            args:
                aws_df: dataframe from the s3 bucket list containing the product weight details.
            returns:
                aws_df: cleaned up dataframe for the product weights in float all in 'kg' metric.
        '''
        aws_df.loc[:,"weight"] = aws_df.loc[:,"weight"].astype(str).apply(lambda x:self.kg_converter(x))
        aws_df.loc[:,"weight"] = aws_df.loc[:,"weight"].astype(str).apply(lambda x:self.multi_g_converter(x))
        aws_df.loc[:,"weight"] = aws_df.loc[:,"weight"].astype(str).apply(lambda x:self.g_converter(x))
        aws_df.loc[:,"weight"] = aws_df.loc[:,"weight"].astype(str).apply(lambda x:self.ml_converter(x))
        aws_df.loc[:,"weight"] = aws_df.loc[:,"weight"].astype(str).apply(lambda x:self.oz_converter(x))
                
        
        aws_df['weight'] = pd.to_numeric(aws_df['weight'], errors='coerce')
    
            
        return aws_df
    
    @staticmethod
    def product_price_converter(s):
        
        '''
        Staticmethod used for applying product price conversion.

            args:
                s: placeholder to check for any value containing '£' sign.
            returns:
                s: keep any value with the '£' same whilst replace gibberish value with nan.
        '''
        if "£" in s:
            return s
        else:
            s = np.nan
            return s
    
    def clean_products_data(self, aws_df):
        
        '''
        Method used for cleaning the product_datas dataframe.

            args:
                aws_df: parameter containing the dataframe for the product details.
            returns:
                aws_df: cleaned up dataframe for the product details.
        '''
        self.convert_product_weights(aws_df)
        aws_df.drop_duplicates()

        aws_df["product_name"] = aws_df["product_name"].astype("string")
        aws_df.loc[:,"product_price"] = aws_df.loc[:,"product_price"].astype(str).apply(lambda x:self.product_price_converter(x))
        aws_df["category"] =  aws_df["category"].astype(str)
        aws_df["EAN"] = pd.to_numeric(aws_df.EAN, downcast="integer", errors='coerce')
        aws_df["date_added"] = pd.to_datetime(aws_df.date_added, errors='coerce', format="mixed")
        aws_df["uuid"] = aws_df["uuid"].astype(str)
        aws_df["product_code"] = aws_df["product_code"].astype(str)

        aws_df.dropna(subset=["weight"], inplace=True)
        # print(aws_df.info())

        return aws_df
    
    def clean_orders_data(self, aws_order_df):
        
        '''
        Method uses the dataframe for order_table containing details for the other dataframe information.
        
            args:
                aws_order_df: dataframe containing the order_table information.
            returns:
                aws_order_df: cleaned dataframe of the order_table information.
        '''
        del aws_order_df["first_name"]
        del aws_order_df["last_name"]
        del aws_order_df["1"]
        del aws_order_df["level_0"]
        aws_order_df.drop_duplicates()

        aws_order_df["date_uuid"] = aws_order_df["date_uuid"].astype(str)
        aws_order_df["store_code"] = aws_order_df["store_code"].astype(str)
        aws_order_df["product_code"] = aws_order_df["product_code"].astype(str)

        aws_order_df.dropna()

        return aws_order_df
    
    @staticmethod
    def time_period_remover(s):

        '''
        Staticmethod used for applying for removing incorrect value.

            args:
                s: placeholder to check for any value containing key word for time_period.
            returns:
                s: keep any key value word same whilst replace gibberish value with nan.
        '''
        if "Evening" in s or "Morning" in s or "Midday" in s or "Late_Hours" in s:
            return s
        else:
            s = np.nan
            return s
    
    
    def clean_json(self, df_json):
        
        '''
        Method uses the dataframe from s3 json file containing information for the date time and product uuid.

            args:
                df_json: paramter containing dataframe json file for time and dates.
            returns:
                df_json: cleaned up dataframe from the json file containing date and time.
        '''
        df_json.drop_duplicates()
    
        df_json["month"] = pd.to_numeric(df_json.month, downcast="integer", errors='coerce')
        df_json["year"] = pd.to_numeric(df_json.year, downcast="integer", errors='coerce')
        df_json["day"] = pd.to_numeric(df_json.day, downcast="integer", errors='coerce')
        df_json.loc[:,"time_period"] = df_json.loc[:,"time_period"].astype(str).apply(lambda x: self.time_period_remover(x))
        df_json["date_uuid"] = df_json["date_uuid"].astype(str)
        
        df_json.dropna(subset=["time_period"], inplace=True)
        
        return df_json