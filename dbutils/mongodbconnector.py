from pymongo import MongoClient
from conf import env
config = env.get_config()

class MongodbConnector():
        
    def __init__(self):
      pass
    
    def get_mongodb_client(self):
        return MongoClient(self.__get_mongodb_host())
      
    def connect_lds_db(self):
        client = MongoClient(self.__get_mongodb_host())
        return client[config.MONGODB_DATABASE_LDS]

    def connect_dataintake_db(self):
        client = MongoClient(self.__get_mongodb_host())
        return client[config.MONGODB_DATABASE]

    def __get_mongodb_host(self):
        if config.MONGODB_HOST and config.MONGODB_PORT:
            if config.MONGODB_USERNAME and config.MONGODB_PASSWORD:
                return f'mongodb://{config.MONGODB_USERNAME}:{config.MONGODB_PASSWORD}@{config.MONGODB_HOST}:{config.MONGODB_PORT}/'
            else :
                return f"{config.MONGODB_HOST}:{config.MONGODB_PORT}/"
        else :
            raise ValueError("Please provide mongodb host and port details.")    