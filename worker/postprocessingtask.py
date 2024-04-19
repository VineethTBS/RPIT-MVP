import os, importlib, sys
from conf import env
from pfapiutils.scheduler.celery.celeryapp import celery
from const import dbconst, errorconst, taskconst
from dbutils.mongodbconnector import MongodbConnector
from pfapiutils.handlers import jobhandler
from pfapiutils.handlers.jobhandler import JobStatus as js

config = env.get_config()
dbconnector = MongodbConnector()

#This task use to do post processing calculations as background task
#This is used for background scheduled task, this method will be invoked by 'CELERY' only
@celery.task(name = taskconst.POST_PROCESSING_TASK_BG, bind=True)
def do_postprocessing_background(self, taskconfig):
    try:
        #get list of pp task (background)
        taskmaps = __get_pptask_maps("background")
        if taskmaps.count() > 0 :
            for taskmap in taskmaps:
                __do_process(taskmap)
        else :
            print(errorconst.TASK_MAP_NOT_AVAILABLE)
    except Exception as ex:
        print(f"{errorconst.POST_PROCESSING_EXCEPTION}{ex}")
        pass

#This task use to do post processing calculations
@celery.task(name = taskconst.POST_PROCESSING_TASK_FG, bind=True)
def do_post_processing_foreground(self, schemaid, srcfilter=None, jobid = None):
    activity = f"Received post processing task for schemaid : {schemaid}"
    try:
        #get list of pp task
        taskmaps = __get_pptask_maps("foreground",srcfilter)
        if taskmaps.count() > 0 :
            for taskmap in taskmaps:
                __do_process(taskmap, schemaid)
            activity = activity + f"\nPost processing completed."
            job_status = js.SUCCESS.value
        else :
            activity = activity + f"{errorconst.TASK_MAP_NOT_AVAILABLE}"
            job_status = js.SUCCESS.value
            print(errorconst.TASK_MAP_NOT_AVAILABLE)
    except Exception as ex:
        activity = activity + f"{errorconst.POST_PROCESSING_EXCEPTION}{ex}"
        job_status = js.FAILED.value
        print(f'{errorconst.POST_PROCESSING_EXCEPTION}{ex}')
    finally:
        if not jobid :
            jobhandler.addtask(taskconst.PROCESS_FILE_TASK, job_status, activity)
        else :
            jobhandler.updatetask(jobid, job_status, activity)
        pass
    
# process pp task
def __do_process(taskmap, schemaid=None):
        if taskmap and taskmap['_tids']:
            sortedlist = sorted( taskmap['_tids'], key=lambda x:x['order'])
            for item in sortedlist:
                task = __get_pptask(item['tid'])
                if task and task['isactive']:
                    callfunc(task['fpath'], task['method'], taskmap['filter'], schemaid)
                else:
                    print(errorconst.ACTIVE_TASK_NOT_AVAILABLE)

#get all post processing task maps
def __get_pptask_maps(ttype, srcfilter =None):
    if srcfilter:
        query = {"isactive": True, "type":ttype, "filter.kw":{"$regex":srcfilter}}
    else:
        query = {"isactive": True, "type":ttype}
    db = dbconnector.connect_dataintake_db()
    col = db[dbconst.PP_TASK_MAP]
    return col.find(query, {"_id":False})

#get task by task id
def __get_pptask(taskid):
    db = dbconnector.connect_dataintake_db()
    col = db[dbconst.PP_TASK]
    return col.find_one({"_id": taskid, "isactive": True}, {"_id":False})

# call function in script
def callfunc(scriptpath, funcname, *args):
    pathname, filename = os.path.split(scriptpath)
    sys.path.append(os.path.abspath(pathname))
    modname = os.path.splitext(filename)[0]
    module = importlib.import_module(modname)
    result = getattr(module, funcname)(*args)
    return result
