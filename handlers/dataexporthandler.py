from const import dbconst
from const import errorconst
from dbutils.mongodbconnector import MongodbConnector
from injector import inject
from bson.objectid import ObjectId

class DataexportHandler():
    
    _PROJECTION_COLUMNS = {
                            "fileattr.sname":True, "sourcepath":True, "othercolumns":True, "rowcount":True,
                            "columnrows":True, "createdat": True, "encoding": True, "isactive":True, "pldt": True,
                            "cldt": True, "country":True, "dealname": True, "dealid": True, "isin":True,
                            "pline": True, "poolid": True, "trnchid":True, "fn": True, "columns": True,
                            "source": True
                          }
    _FILTER_COLUMNS = {"dealid": "dealid", "poolid": "poolid", "sourceid": "sourceid"}
    
    @inject
    def __init__(self, mongodbconnector: MongodbConnector):
      self.mongodbconnector = mongodbconnector
      
    
    def get_available_poolcuts(self, uidtype, uid, poolcutdate, filter, projectioncolumns = _PROJECTION_COLUMNS):
      srcolumn = self._get_sourceid_column_name(uidtype)
      if uid.isdigit():
        uid = int(uid)
      db = self.mongodbconnector.connect_lds_db()
      col = db[dbconst.FILE_METADATA]
      if poolcutdate is '':
        return list(col.find({srcolumn: uid},  projectioncolumns))
      else:
        filtercondition = '$' + filter
        #return list(col.find({ '$and' : [ {srcolumn: uid} , {"pldt" : {filtercondition: poolcutdate}} ] }, {self._PROJECTION_COLUMNS} ))
        return col.find({ srcolumn: uid, "pldt" : {filtercondition: poolcutdate}}, projectioncolumns)

    def download_rawdata(self, file_metadataids, collection, return_columns):
      db = self.mongodbconnector.connect_lds_db()
      col = db[collection]
      return_columns = {return_column.upper():True for return_column in return_columns}
      return_columns.update( {'_id' : False} )
      if type(file_metadataids) is not list:
        file_metadataids = [file_metadataids]
      return col.find({"_s": { '$in': file_metadataids } }, return_columns)
    
    def get_filemetadata_byid(self, fmetaid):
      if not isinstance(fmetaid, ObjectId): 
          fmetaid = ObjectId(fmetaid)
      db = self.mongodbconnector.connect_lds_db()
      col = db[dbconst.FILE_METADATA]
      return col.find_one({"_id": fmetaid})
    
    # This method returns 'db column name' for given uidtype
    def _get_sourceid_column_name(self, uidtype):
      if uidtype.lower() in self._FILTER_COLUMNS:
        return self._FILTER_COLUMNS[uidtype.lower()]
      else:
        raise Exception(errorconst.UNKNOWN_UNIQUE_IDENTIFIER_TYPE)