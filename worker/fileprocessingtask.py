from pfapiutils.scheduler.celery.celeryapp import celery
from conf import env
from pfapiutils.sftp.handlerfactory import HandlerFactory
from pymongo import MongoClient
from const import dbconst, errorconst, taskconst
from pfapiutils.handlers import jobhandler, filedataimporthandler
from pfapiutils.handlers.jobhandler import JobStatus as js
from dbutils.mongodbconnector import MongodbConnector
from datetime import datetime
import os, calendar

config = env.get_config()
dbconnector = MongodbConnector()

#This task use to process file it.
@celery.task(name = taskconst.PROCESS_FILE_TASK, bind=True)
def process_file(self, fileattr, srcconfig, taskconfig, jobid = None):
    activity = f"Received task for processing file: {fileattr['lname']}"
    try:
        #process file
        filemetadataid =  filedataimporthandler.doprocess(srcconfig["destination"] + "/"+ fileattr["lname"], 
                                                            srcconfig["name"], fileattr["src"], fileattr, 
                                                            jobid if jobid else self.request.id)
        activity += f"\nData import completed for file {fileattr['lname']}"
        job_status = js.SUCCESS.value      
        return filemetadataid
    except Exception as e:
        print(e)
        activity = activity + f"{activity}{e}"
        job_status = js.FAILED.value
        #configure retry
        if taskconfig and taskconfig["retry"]["enabled"] :
            self.retry(countdown=taskconfig["retry"]["countdown"] if taskconfig["retry"]["countdown"] else 60,
                       max_retries=taskconfig["retry"]["maxtry"] if taskconfig["retry"]["maxtry"] else 1,
                       args=[fileattr, srcconfig, taskconfig, self.request.id])
        #TODO : logger needs to be called
        #TODO : email needs to go to dev team
    finally:
        if not jobid :
            jobhandler.addtask(taskconst.PROCESS_FILE_TASK, job_status, activity)
        else :
            jobhandler.updatetask(jobid, job_status, activity)
        pass

#This task use to download file from server.
@celery.task(name = taskconst.DOWNLOAD_FILE_TASK, bind=True)
def download_file(self, fileattr, srcconfig, taskconfig, jobid = None):
    activity = f"Received task for download file: {fileattr['lname']} TS: {datetime.now()}"
    try:
        if srcconfig:
            #get handler for file server
            handler = HandlerFactory().get_handler(srcconfig)
            
            #download file from file server
            if srcconfig["rules"]:            
                handler.downloadfile(fileattr["src"], srcconfig["destination"], fileattr["sname"], fileattr["lname"],
                                 retainfolderstructure=srcconfig["rules"]["retainfolderstructure"],
                                 overridefile=srcconfig["rules"]["overridefile"])
            else:
                 handler.downloadfile(fileattr["src"], srcconfig["destination"], fileattr["sname"], fileattr["lname"])
                 
            activity = activity + f"\nFile : {fileattr['lname']} downloaded successfully."
            job_status = js.SUCCESS.value
        else :
            print(errorconst.REMOTE_CONFIG_UNAVAILABLE)
            activity = activity + f"\nNo source config available for File : {fileattr['lname']}."
    except Exception as e:
        print(e)
        activity = activity + f"{activity}{e}"
        job_status = js.FAILED.value
        #configure retry
        if taskconfig and taskconfig["retry"]["enabled"] :
            self.retry(countdown=taskconfig["retry"]["countdown"] if taskconfig["retry"]["countdown"] else 60,
                       max_retries=taskconfig["retry"]["maxtry"] if taskconfig["retry"]["maxtry"] else 1,
                       args=[fileattr, srcconfig, taskconfig, self.request.id])
        #TODO : logger needs to be called
        #TODO : email needs to go to dev team
    finally:
        if not jobid :
            jobhandler.addtask(taskconst.PROCESS_FILE_TASK, job_status, activity)
        else :
            jobhandler.updatetask(jobid, job_status, activity)
        pass

#this task use to get list of files for server, which needs to be download and process.
#It also create individual task per file.
@celery.task(name = taskconst.GET_FILES_LIST_SFTP_TASK, bind=True)
def get_files_for_processing(self, taskconfig, jobid = None):
    activity = f"Received task {taskconst.GET_FILES_LIST_SFTP_TASK} to get list of available files."
    try:
        #get list of ftp/sftp config(s) available
        configs = __getconfigs()
        if configs.count() > 0 :
            for srcconfig in configs:
                handler = HandlerFactory().get_handler(srcconfig)
                listfileattr, srcconfig = __queuefiles(srcconfig, handler)
        else :
            activity = activity + f"{errorconst.REMOTE_CONFIG_UNAVAILABLE} .Task completed."
            print(errorconst.REMOTE_CONFIG_UNAVAILABLE)
            return None, None
        
        activity = activity + " Task completed."
        job_status = js.SUCCESS.value
        return listfileattr, srcconfig
    except Exception as e:
        print(e)
        activity = activity + f"{activity}{e}"
        job_status = js.FAILED.value
        #configure retry
        if taskconfig["retry"] and taskconfig["retry"]["enabled"] :
            self.retry( countdown=taskconfig["retry"]["countdown"] if taskconfig["retry"]["countdown"] else 60, 
                        max_retries=taskconfig["retry"]["maxtry"] if taskconfig["retry"]["maxtry"] else 1,
                        args=[taskconfig, self.request.id])
        
        #TODO : logger needs to be called
        #TODO : email needs to go to dev team
    finally:
        if not jobid :
            jobhandler.addtask(taskconst.PROCESS_FILE, job_status, activity)
        else :
            jobhandler.updatetask(jobid, job_status, activity)
        pass

#This task use to retry failed "file processing" task.
@celery.task(name = taskconst.RETRY_FAILED_PROCESSFILE_TASK, bind=True)
def processfile_retry(self, taskconfig):
    try:
        query = [{"$lookup": {"from": "jobs", "localField": "_tid", "foreignField": "tid", "as": "failedtask"}},
                 {"$match" : { "$and" : [   { "failedtask.status" : {"$eq": js.FAILED.value}},
							                { "failedtask.tname" : {"$eq" : taskconst.PROCESS_FILE_TASK}}]}
                }]
        client = MongoClient(f"{config.MONGODB_HOST}:{config.MONGODB_PORT}/")
        db = client[config.MONGODB_DATABASE_LDS]
        filemetalist = list(db[dbconst.FILE_METADATA].aggregate(query))
        if len(filemetalist)>0:
                print(f"'ProcessFile' failed task count is: {len(filemetalist)}")
                for task in filemetalist :
                    srcconfig = __getconfigbyname(task["source"])
                    #sending task to the celery for processing
                    celery.send_task(taskconst.PROCESS_FILE_TASK, args=[task["fileattr"], srcconfig, taskconfig, task["_tid"]])
        else:
            print(f"No 'ProcessFile' failed task found to retry.")
    except Exception as e:
        print(f"{errorconst.ERROR_GET_FAILED_TASK}{e}")
        pass

def __getconfigs():
    try:
        db = dbconnector.connect_dataintake_db()
        col = db[dbconst.SFTP_CONFIG]
        return col.find({"isactive": True},{"_id":False})
    except Exception as e:
        print(f"{errorconst.SFTP_LIST_ERROR}{e}")
        pass
    
def __getconfigbyname(name):
    try:
        db = dbconnector.connect_dataintake_db()
        col = db[dbconst.SFTP_CONFIG]
        return col.find_one({"name": name},{"_id":False})
    except Exception as e:
        print(f"{errorconst.SFTP_LIST_ERROR}{e}")
        pass

#this method is used to download files from SFTP/FTP servers
def __downloadfile(fileattr, config, handler, tid=None):
    try:
        #download file from sftp
        if config["rules"]["sourceconfig"]:            
            handler.downloadfile(fileattr["src"], config["destination"], fileattr["sname"], fileattr["lname"],
                                 retainfolderstructure=config["rules"]["retainfolderstructure"],
                                 overridefile=config["rules"]["overridefile"])
        if tid :
            jobhandler.updatetask(tid, "InProgress", "downloading of file completed") 
    except OSError as e:
        print(e)
        pass
    except Exception as e:
        print(e)
        #TODO : logger needs to be called
        #TODO : email needs to go to dev team 

#this method will get list of files from SFTP/FTP and send each file in queue for further processing.
def __queuefiles(config, handler):
    try:
        #get source based on configuration
        src = __getsource(config)
              
        #get list of file(s) attributes from source
        fileattrs = handler.get_file_attributes(src, filter=config["rules"]["filefilter"])
        
        #take actions based on rules configured
        if not config["rules"]["retainfolderstructure"] :
            if(config["rules"]["handlefilewithsamename"]):
                sortedlist = sorted(fileattrs, key=lambda k:(k["sname"],k["src"]))
                fileattrs = []; i=1; prevname = ""
                for attr in sortedlist:
                    currname = attr["sname"]
                    if prevname.lower() == currname.lower() :
                        name = str(attr["sname"])
                        index = name.find(".")
                        fileattrs.append({"src":attr["src"], "sname": attr["sname"], "lname": name[:index]+" - ("+str(i)+ ")"+name[index:], "dtmodified" : attr["dtmodified"]})
                        i+=1
                    else :
                        prevname = currname
                        i=1
                        fileattrs.append(attr)
        if config["rules"]["downloadlatestonly"] :
            filelist = []
            for attr in fileattrs:
                localpath = config["destination"] + "/"+ attr["src"]+"/"+attr["sname"] if config["rules"]["retainfolderstructure"] else config["destination"]+"/"+attr["lname"]
                if os.path.exists(localpath):
                    if datetime.fromtimestamp(os.path.getmtime(localpath)) < attr["dtmodified"] :
                        filelist.append(attr)
                else :
                    filelist.append(attr)
            fileattrs = filelist
        print(f"file(s) count is: {len(fileattrs)}")
        return fileattrs, config
    except Exception as e:
        print(repr(e))        
        #TODO : logger needs to be called
        #TODO : email needs to go to dev team
        pass

# This method calculate source path based on config provided
def __getsource(config):
    if config["rules"]["sourceconfig"]["isactive"]:
        if config["rules"]["sourceconfig"]["format"] :
            now = datetime.now()
            f = config["rules"]["sourceconfig"]["format"]
            if "{yyyy}" in f or "{yy}" in f:
                f=f.replace("{yyyy}",   str(now.year)+"/")
                f=f.replace("{yy}",  str(now.year)+"/")
            if "{mmm}" in f:
                f=f.replace("{mmm}", str(calendar.month_name[now.month])+"/")
            if "{mm}" in f:
                f=f.replace("{mm}",  "{:02d}".format(now.month)+"/")
            if "{dd}" in f :
                f =f.replace("{dd}",  "{:02d}".format(now.day-1)+"/")
            return f
        else :
            return config["source"]
    else :
        return config["source"]
    
def __createactivitylog(tid, tname, status, activity):
    return {"tid":tid, "tname":tname, "status":status, "activity":activity}