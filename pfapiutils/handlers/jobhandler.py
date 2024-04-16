import uuid
from conf.base import BaseConfig
from datetime import datetime
from const import dbconst, taskconst
from pymongo import MongoClient
from enum import Enum
from pfapiutils.scheduler.celery.celeryapp import celery

class JobStatus(Enum):
        PENDING = 1
        INPROGRESS = 2
        SUCCESS = 3
        FAILED = 4    

config = BaseConfig()

@celery.task(name = taskconst.ADD_JOB)
def addtask(tname, status, desc, add_ts = True):
        client = MongoClient(f"{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        db = client[config.MONGODB_DATABASE_LDS]
        col = db[dbconst.JOBS]
        desc = str(desc + f" TS : {datetime.now()}") if add_ts else str(desc)
        tid = str(uuid.uuid4())
        col.insert({"tid": tid,"tname": tname, "status" : status, "desc":str(desc), "tsupdated" : datetime.now()})
        return tid

@celery.task(name = taskconst.GET_JOB)
def gettask(tid):
        client = MongoClient(f"{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        db = client[config.MONGODB_DATABASE_LDS]
        col = db[dbconst.JOBS]
        col.find({"tid": tid})

@celery.task(name = taskconst.UPDATE_JOB)      
def updatetask(tid, status, desc, add_ts = True, append_desc=True):
        client = MongoClient(f"{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        db = client[config.MONGODB_DATABASE_LDS]
        col = db[dbconst.JOBS]
        desc = str(desc + f" TS : {datetime.now()}") if add_ts else str(desc)
        if append_desc:
                record = col.find_one({"tid":tid})
                desc = record["desc"] + "\n" + desc
        col.update({"tid": tid},{"$set":{"desc": desc, "status" : status, "tsupdated" : datetime.now()}})