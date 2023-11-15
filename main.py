from database_utils import *
from data_extraction import *
from data_cleaning import *


'''
This main.py is used to test each methods from different class.
'''


if __name__ == "__main__":
    object = DatabaseConnector
    print(object.read_db_creds())
    print(object.init_db_engine())