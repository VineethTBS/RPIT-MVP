import os, importlib, sys
import re
import pandas as pd
import numpy as np
import pymongo as pm
from pymongo.errors import BulkWriteError
from datetime import datetime
from bson.objectid import ObjectId
from dbutils.mongodbconnector import MongodbConnector

#add current working directory in system path
sys.path.append(os.path.abspath(os.getcwd()))

#import modules
env = importlib.import_module("conf.env")
dbconst = importlib.import_module("const.dbconst")

config = env.get_config()
dbconnector = MongodbConnector()
db_lds = dbconnector.connect_lds_db()

#calculate edcode from filename for all
def get_edcode_from_file(filter,args):
    if filter and filter["kw"] :
        query = {"$and" :[{"filename":{"$regex":filter["kw"]}},{"$or":[{"sourceid":None}, {"sourceid":''}]}] }
    else :
        query = {"$or":[{"sourceid":None}, {"sourceid":''}]}
    coll = db_lds[dbconst.FILE_METADATA]
    metadatalist = coll.find(query,{"_id":True, "filename" : True})
    if metadatalist.count() > 0 :
        for data in metadatalist:
            f = data["filename"]
            i = f.rfind("/")
            j = f.find("_")
            edcode = f[i+1:j]
            coll.update_one({"_id":data["_id"]}, {"$set":{"sourceid" : edcode}})
            print(f"SourceId updated in {config.MONGODB_DATABASE_LDS}.{dbconst.FILE_METADATA} for file {f}")
    else:
        print("No SourceId to update")
    return True

#calculate cutoff date from filename for all
def get_poolcut_date_from_file(filter, args):
    if filter and filter["kw"] :
        query = {"$and" :[{"filename":{"$regex":filter["kw"]}},{"$or":[{"pldt":None}, {"pldt":''}]}] }
    else :
        query = {"$or":[{"pldt":None}, {"pldt":''}]}
    coll = db_lds[dbconst.FILE_METADATA]
    metadatalist = coll.find(query,{"_id":True, "filename" : True})
    if metadatalist.count() > 0 :
        for data in metadatalist:
            f = data["filename"]
            findex = f.find("_")
            lindex = f.rfind("_")
            sdate = f[findex+1:lindex]
            pldt = datetime.strptime(sdate, "%Y-%m-%d")
            coll.update_one({"_id":data["_id"]}, {"$set":{"pldt" : pldt}})
            print(f"Poolcutdate updated in {config.MONGODB_DATABASE_LDS}.{dbconst.FILE_METADATA} for file {f}")
    else:
        print("No poolcutdate to update")
    return True

#calculate other details like 'sourceid', 'pldt', 'dealid', 'poolid' etc
def calculate_details_byid(filter, args):
    if not isinstance(args, ObjectId): 
          args = ObjectId(args)
    coll = db_lds[dbconst.FILE_METADATA]
    filemetadata = coll.find_one({"_id":args},{"_id":True, "filename" : True})
    if filemetadata :
        filename = filemetadata["filename"]
        pldt = _get_poolcutdate_from_filename(filename)
        edcode = _get_edcode_from_filename(filename)
        coll.update_one({"_id":filemetadata["_id"]}, {"$set":{"pldt" : pldt, "sourceid":edcode}})
        print(f"Poolcutdate and SourceId updated for file {filename}")
        
        #check and handle if file is already processed
        _handle_duplicatefile(filemetadata["_id"], pldt, edcode)
        
        # calculate poolcut sequence
        #_calculate_poolcut_sequence()
        
        #this has to be done at last
        _update_filemetadata_details(args, filename)
    else:
        print(f"No File Metadata found with id {args}")
        
#calculate edcode from filename
def _get_edcode_from_filename(filename):
    if filename:
            i = filename.rfind("/")
            j = filename.find("_")
            edcode = filename[i+1:j]
            return edcode            
    else:
        return None

#calculate cutoff date from filename
def _get_poolcutdate_from_filename(filename):
    if filename :
            findex = filename.find("_")
            lindex = filename.rfind("_")
            sdate = filename[findex+1:lindex]
            pldt = datetime.strptime(sdate, "%Y-%m-%d")
            return pldt            
    else:
        return None

#update details like DealId, PL, TrnchId, PoolId, ClDt, ISIN, DName 
def _update_filemetadata_details(fmetaid, filename):
    query = [{"$match" : {"_id":{"$eq":fmetaid}}},
             {"$lookup": {"from": "lkp_deal", "localField": "sourceid", "foreignField": "encode", "as": "lisin"}},
             {"$unwind": {"path": "$lisin", "preserveNullAndEmptyArrays": False}},
             {"$unwind": {"path": "$lisin.ISIN", "preserveNullAndEmptyArrays": False}},
             {"$lookup": {"from": "SFGDATA", "localField": "lisin.ISIN", "foreignField": "ISIN", "as": "mdeal"}},
             {"$unwind": {"path": "$mdeal","preserveNullAndEmptyArrays": False}}
            ]
    filemetalist = list(db_lds[dbconst.FILE_METADATA].aggregate(query))
    if len(filemetalist) > 0 :
        fmeta  = filemetalist[0]
        db_lds[dbconst.FILE_METADATA].update_one({"_id":fmeta["_id"]},
            {"$set":{"dealid" : fmeta["mdeal"]["DId"], "pline":fmeta["mdeal"]["PL"], "trnchid":fmeta["mdeal"]["TId"], 
            "isin":fmeta["lisin"]["ISIN"], "cldt" : fmeta["mdeal"]["ClDt"], "poolid": fmeta["mdeal"]["PId"],
            "dealname":fmeta["mdeal"]["DName"],"country":fmeta["mdeal"]["AD"]}}, False)
        print(f"DealId, PL, TrnchId, PL, PoolId, ISIN, DealName updated for {filename}")
    else :
        print(f"No DealId, PL, TrnchId, PL, PoolId, ISIN, DealName found for {filename}")
        
# check and handle if file is already processed.
# if file is already processed earlier then mark older filemetadata id(s) inactive 
def _handle_duplicatefile(filemetaid, pldt, sourceid):
    metadatalist = db_lds[dbconst.FILE_METADATA].find({"$and":[{"sourceid":{"$eq":sourceid}},
                                                               {"pldt":{"$eq":pldt}},{"_id":{"$ne":filemetaid}}]})
    for metadata in metadatalist:
        db_lds[dbconst.FILE_METADATA].update_one({"_id":metadata["_id"]}, {"$set":{"isactive":False}})
        print(f"file {metadata['filename']} already processed with file metadata id {metadata['_id']},"
              "so marked it inactive")

# method use to calculate latest and closing poolcut sequence
# apply the logic based on edcode or moodys dealid -- key
# Get all unique keys
# loop on each unique key and identify other keys based on poolcutoff date asc/desc
# Update it back to database
def _calculate_poolcut_sequence():
    data = _get_metaData('')
    datadf =  pd.DataFrame(data)
    unique_edw_ids = datadf['sourceid'].unique().tolist()
    datadf['closingpoolcutseq'] = None
    datadf['latestpoolcutseq'] = None
    alloperations = []
    for unique_edw_id in unique_edw_ids:
        if not str(unique_edw_id)=='nan':
            df = datadf[(datadf['sourceid'] == unique_edw_id)]
            df = df.sort_values('createdat')
            tmp = df.groupby('sourceid').size()
            rank = tmp.map(range)
            rank =[item for sublist in rank for item in sublist]
            df['closingpoolcutseq'] = rank
            df = df.sort_values('createdat',ascending=False)
            tmp = df.groupby('sourceid').size()
            rank = tmp.map(range)
            rank =[item for sublist in rank for item in sublist]
            df['latestpoolcutseq'] = rank
            finaldf = df.dropna(axis=1,how='all') #explicitly delete all empty columns to reduce space in mongo
            finaldf = finaldf.replace(np.nan, '', regex=True)
            my_list1 = finaldf.to_dict(orient='records')
            my_list = [{k:v for k,v in d.items() if v != '' } for d in my_list1]
            operations = [pm.operations.ReplaceOne(
                filter={"_id": doc["_id"]}, 
                replacement=doc
                ) for doc in my_list]
            alloperations += operations
            print(unique_edw_id)

    result = db_lds[dbconst.FILE_METADATA].bulk_write(alloperations)
    print("result")

def _get_metaData(filter):
    if filter:
        cur = db_lds[dbconst.FILE_METADATA].find({filter}) # {'_id' : 1 , 'fn' : 1, 'createdat':1 }
    else:
        cur = db_lds[dbconst.FILE_METADATA].find({}) #{'_id' : 1 , 'fn' : 1, 'createdat':1 }
    return list(cur)

def _execute_LobQueries_ForFile(filter, args):
    coll = db_lds[dbconst.FILE_METADATA]
    filemetadata = coll.find_one({"_id":args},{"_id":True, "filename" : True})
    if filemetadata :
            dealid = filemetadata["Did"]
            historyData = _get_filemetadata({"Did":dealid}, {"_id":True, "Did" : True, "pldt":True})
            if historyData.count() > 0 :
                lCutoff = historyData[0]["pldt"]
                pCutoff = historyData[1]["pldt"]
                fCutoff = historyData[len(historyData)-1]["pldt"]
            else:
                print("No History Data found")
                lCutoff = filemetadata["pldt"]
                pCutoff = None
                fCutoff = None
            _executequries(lCutoff, pCutoff, fCutoff)
    else:
        print(f"No file metadata found for {args}")
    
def _get_filemetadata(query, projection=None):
    if query :
        coll = db_lds[dbconst.FILE_METADATA]
        return coll.find(query, projection).sort("pldt", -1)
    else:
        return None
    
def _executequries(lCutoff, pCutoff, fCutoff):
    db = dbconnector.connect_dataintake_db()
    queryConfigs = db["lob_query"].find({"isExec": True }).sort("exec_seq", 1)
    if queryConfigs.count() > 0 :
        for queryConfig in queryConfigs :
            queries = queryConfig["query"].split("||")
            projections = queryConfig["projection"].split("||")
            targetCollections = queryConfig["tar_coll"].split("||")
            save_results(queryConfig,queries,projections,targetCollections,db,lCutoff)
    else :
        print("No Query found in collection")
    
def save_results(queryConfig,queries,projections,targetCollections,db,lCutoff):
    if "formula" not in  queryConfig:
        res=db.command(queryConfig["cmd_type"],targetCollections[0],pipeline=eval(queries[0]),cursor={})
        if(len(res["cursor"]["firstBatch"])>0) :
            d = res["cursor"]["firstBatch"][0]
            #saving results to db
            db[queryConfig["res_coll"]].save({"name" : queryConfig["name"], "result": d[projections[0]],
                                              "cutoffDate" : lCutoff, "tar_coll" : queryConfig["tar_coll"],
                                               "timestamp": datetime.now()})
        else :
            db[queryConfig["res_coll"]].save({"name" : queryConfig["name"], "result": 0.0,
                                                      "cutoffDate" : lCutoff, "tar_coll" : queryConfig["tar_coll"],
                                                      "timestamp": datetime.now()})
    else :
        formula = queryConfig["formula"]
        for x in range(len(queries)):
            res=db.command(queryConfig["cmd_type"],targetCollections[x],pipeline=eval(queries[x]),cursor={})
            if len(res["cursor"]["firstBatch"])>0 : 
                for d in res["cursor"]["firstBatch"] :
                    formula = re.sub(rf"\b{str(projections[x])}\b",str(d[projections[x]]), formula, flags=re.IGNORECASE)
            else:
                formula = re.sub(rf"\b{str(projections[x])}\b","0.0", formula, flags=re.IGNORECASE)
        #saving results to db
        db[queryConfig["res_coll"]].save({"name" : queryConfig["name"], "result": eval(formula), "cutoffDate" : lCutoff, "tar_coll" : queryConfig["tar_coll"], "timestamp": datetime.now()})