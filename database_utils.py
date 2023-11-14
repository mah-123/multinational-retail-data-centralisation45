import yaml

class DatabaseConnector():
    
    def __init__(self):
        pass

    def read_db_creds(self):
        with open("db_creds.yaml") as file:
            dict = yaml.safe_load(file)
        return dict

    def init_db_engine(self):
        pass

    def upload_to_db(self):
        # dim_card_details
        pass