
from pfapiutils.scheduler.celery.celeryapp import celery
from conf import env
from const import dbconst, taskconst
from datetime import datetime
from . import taskchain
from dbutils.mongodbconnector import MongodbConnector

config = env.get_config()
dbconnector = MongodbConnector()

#This method is use to schedule task(s) available at database.
#It will be invoked by celery beat
@celery.task(name = taskconst.SCHEDULE_TASK , bind=True)
def scheduletask(self):
    try:
        taskconfigs = __getschedulabletasks()
        if taskconfigs.count() > 0 :
            for taskconfig in taskconfigs:
                if __istaskschedulable(taskconfig["schedule"]):
                    taskchain.invoke(taskconfig["method"], taskconfig)
        else:
            print("No task available to schedule")
    except Exception as e:
        print(e)
        pass
    
def __getschedulabletasks():
    db = dbconnector.connect_dataintake_db()
    col = db[dbconst.TASK]
    return col.find({"isactive": True}, {"_id":False})

def __istaskschedulable(schedule):
    schedulable = True
    now = datetime.now()
    if schedule:
        if(schedule["weekday"]):
            if str(now.weekday()) in schedule["weekday"] or schedule["weekday"] =="*":
                pass
            else:
                schedulable = False
        if(schedule["hour"] and schedulable):
            if str(now.hour) in schedule["hour"] or schedule["hour"] =="*":
                schedulable = True
            else:
                schedulable = False
        if(schedule["minute"] and schedulable):
            if str(now.minute) in schedule["minute"] :
                return True
            for m in str(schedule["minute"]).split(","):
                val = int(m)
                if now.minute > val and (now.minute - val) < config.TASK_MINUTE :
                    return True
            schedulable = False
        return schedulable
    else:
        return False