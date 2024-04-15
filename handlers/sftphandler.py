from const import dbconst
from const import errorconst
from dbutils.mongodbconnector import MongodbConnector
from injector import inject
from bson.objectid import ObjectId
from bson.json_util import dumps

class SftpHandler():
    @inject
    def __init__(self, mongodbconnector: MongodbConnector):
      self.mongodbconnector = mongodbconnector
    
    def get_dataintake_byid(self, id,filtertable):
      if not isinstance(id, ObjectId): 
          id = ObjectId(id)
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[filtertable]
      return col.find_one({"_id": id,"isdeleted":False})
      
    def post_sftpdata_byid(self,sftpid,source,istlsenable):
      if not isinstance(sftpid, ObjectId): 
          sftpid = ObjectId(sftpid)
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[dbconst.SFTP_CONFIG]  
      col.update_one({"_id": sftpid},{"$set":{"source" : source,"isTLSenable":istlsenable}})
      return col.find_one({"_id": sftpid}) 

    def get_dataintake_data(self,filtertable):
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[filtertable]
      return col.find({"isdeleted":False}).sort("name")
    
    def delete_dataintake_data(self,sftpid,filtertable):
      if not isinstance(sftpid, ObjectId): 
          sftpid = ObjectId(sftpid)
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[filtertable]
      col.update_one({"_id": sftpid},{"$set":{"isdeleted" : True}})
      return "Deleted Successfully"
          
    def check_deleted_dataintake_data_byid(self, sftpid,filtertable):
      if not isinstance(sftpid, ObjectId): 
        sftpid = ObjectId(sftpid)
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[filtertable]
      return col.find_one({"_id": sftpid})

    def create_sftp_date(self,source,istlsenable,protocol,retainfolderstructure,handlefilewithsamename,overridefile):
      db = self.mongodbconnector.connect_dataintake_db()
      col = db[dbconst.SFTP_CONFIG]
      data_dict={"name":"local","source":source,"rules":{"backupoldfile":False,
                  "retainfolderstructure":retainfolderstructure,"handlefilewithsamename":handlefilewithsamename,"overridefile":overridefile},"isTLSenable":istlsenable,
                  "protocol":protocol,"isdeleted":False}
      id=col.insert_one(data_dict)
      return "New document has been created successfully in SFTP collection and new id is " + str(id.inserted_id)