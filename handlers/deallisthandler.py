from const import dbconst
from dbutils.mongodbconnector import MongodbConnector
from injector import inject

class DealListHandler():
    @inject
    def __init__(self, mongodbconnector: MongodbConnector):
      self.mongodbconnector = mongodbconnector
    
    def get_deallistdata(self):
      db = self.mongodbconnector.connect_lds_db()
      col = db[dbconst.IKP_DEAL]
      '''Getting only required fields'''
      return col.find({},{"encode":1,"dealname":1,"ISIN":1,"isactive":1,"isprovisional":1})