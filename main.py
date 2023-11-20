from database_utils import *
from data_extraction import *
from data_cleaning import *


'''
This main.py is used to test each methods from different class.
'''


if __name__ == "__main__":
    object = DatabaseConnector()
    obj2 = DataExcractor()
    obj3 = DataCleaning()
    print(object.read_db_creds())
    print(object.init_db_engine())
    print(obj2.list_db_tables())
    print(obj2.read_rds_table('legacy_users'))
    obj3.clean_user_data()
    # object.upload_to_db('dim_users')
