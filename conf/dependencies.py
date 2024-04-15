from injector import singleton
from processors.dataexportprocessor import DataexportProcessor
from handlers.dataexporthandler import DataexportHandler
from dbutils.mongodbconnector import MongodbConnector
from handlers.sftphandler import SftpHandler
from processors.sftpprocessor import SftpProcessor
from handlers.deallisthandler import DealListHandler
from processors.deallistprocessor import DealListProcessor

def configure_di(binder):
    binder.bind(DataexportHandler, to=DataexportHandler, scope=singleton)
    binder.bind(MongodbConnector, to=MongodbConnector, scope=singleton)
    binder.bind(DataexportProcessor, to=DataexportProcessor, scope=singleton)
    binder.bind(SftpHandler,to=SftpHandler,scope=singleton)
    binder.bind(SftpProcessor,to=SftpProcessor,scope=singleton)
    binder.bind(DealListHandler,to=DealListHandler,scope=singleton)
    binder.bind(DealListProcessor,to=DealListProcessor,scope=singleton)