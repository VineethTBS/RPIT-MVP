from .base import BaseConfig

class StagingConfig(BaseConfig):
    # config for staging environment
    MONGODB_HOST = 'ftc-lbsndbox890'
    MONGODB_PORT = '49001'
    MONGODB_DATABASE = 'data_intake'
    MONGODB_USERNAME = 'poctest'
    MONGODB_PASSWORD = 'poctest'
    MONGODB_DATABASE_LDS = 'LoanDataStore'