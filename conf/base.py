import os

class BaseConfig():

   PROJECT_NAME = "pfapi"

   # Get app root path, also can use flask.root_path.
   PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

   #logging setting
   LOGGING_FORMAT   = '%(asctime)s %(levelname)-8s %(correlationId)s %(message)s'
   LOGGING_LOCATION =  'logs/application.log'
   LOGGING_LEVEL = 'DEBUG'

   #API name settings
   API_URL_PREFIX = '/v1/api'
   API_VERSION = '0.1'
   BLUEPRINT_NAME = 'API v1'
   
   #sftp settings
   SFTP_HOST_FILE_PATH = 'C:/Users/KURUVAR/.ssh/known_hosts'
   
   #mongodb settings
   MONGODB_HOST = 'mongodb://localhost'
   MONGODB_PORT = '27017'
   MONGODB_DATABASE = 'data_intake'
   MONGODB_USERNAME = ''
   MONGODB_PASSWORD = ''
   MONGODB_DATABASE_LDS = 'LoanDataStore'
   #task scheduler settings
   TASK_HOUR = 0
   TASK_MINUTE = 15
   TASK_SECONDS = 0
  
   #celery settings
   CELERY_BROKER_URL = 'amqp://'
   CELERY_RESULT_BACKEND = 'mongodb://localhost:27017/task_results'

   #swagger settings
   SWAGGER_HOST = 'localhost:5000'
   SWAGGER_DESCRIPTION = BLUEPRINT_NAME
   SWAGGER_PRODUCES = ["application/json"]

   SWAGGER_TEMPLATE = {
   "swagger": "2.0",
   "info": {
    "title": "Raw Data Intake (RDI)",
    "description": "APIs for raw data intake",
    "contact": {
      "responsibleOrganization": "MIS",
      "responsibleDeveloper": "ATG",
      "email": "sfgtechbangalore@moodys.com",
    },
    "version": "0.0.1"
   },
   "host": SWAGGER_HOST,  # overrides localhost:500
   "basePath": "/",  # base bash for blueprint registration
   "schemes": [
    "http",
    "https"]
   }
   
   RAW_FILE_TEMP_LOCATION = "C:/DataIntake/rawfiles/"
  
