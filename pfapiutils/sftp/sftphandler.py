from stat import S_ISDIR, S_ISREG
import os, re
from datetime import datetime
from shutil import move
from const import errorconst

class SftpHandler() :
    
    def __init__(self, FSConnector) :
        self.sftp = FSConnector.connect()
    
    #recursive download pysftp.get_r() don't work properly with windows, refere below link for more details
    #https://stackoverflow.com/questions/50118919/python-pysftp-get-r-from-linux-works-fine-on-linux-but-not-on-windows
                 
    def recursive_download(self, source, destination, filter=None, preserve_mtime = False) :
        if self.sftp.exists(source):
            if not os.path.exists(destination):
                os.makedirs(destination)
            for entry in self.sftp.listdir_attr(source):
                remotepath = source + "/" + entry.filename
                localpath = os.path.join(destination, entry.filename)
                mode = entry.st_mode
                if S_ISDIR(mode):
                    try:
                        os.mkdir(localpath)
                    except OSError:     
                        pass
                    self.recursive_download(remotepath, localpath, filter, preserve_mtime)
                elif S_ISREG(mode):
                    if filter :
                        match = re.search(filter, entry.filename, re.IGNORECASE)
                        if match:
                            self.sftp.get(remotepath, localpath, preserve_mtime=preserve_mtime)
                    else:
                        self.sftp.get(remotepath, localpath, preserve_mtime=preserve_mtime)
        else :
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
        
    def movefiles(self, sourcedir, destinationdir) :
        if os.path.exists(sourcedir):
            if not os.path.exists(destinationdir):
                os.makedirs(destinationdir)
            for file in os.listdir(sourcedir):
                if os.path.isfile(os.path.join(sourcedir, file)):
                    move(os.path.join(sourcedir, file),os.path.join(destinationdir, os.path.basename(file)))
                    print( f"Moved file {destinationdir}/{file}")
                else:
                    self.movefiles(os.path.join(sourcedir, file), os.path.join(destinationdir, file))
        else:
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
    
    #This method returns list of 'file attributes' of file    
    def get_file_attributes(self, source, filter=None, files=None):
        if not files:
            files = []   
        if self.sftp.exists(source):
            for entry in self.sftp.listdir_attr(source):
                remotepath = source + "/" + entry.filename
                mode = entry.st_mode
                if S_ISDIR(mode) :
                    files = self.get_file_attributes(remotepath, filter, files)
                elif S_ISREG(mode):
                    if filter :
                        match = re.search(filter, entry.filename, re.IGNORECASE)
                        if match:
                            print(f"matched file is {entry.filename}")
                            files.append({"src":source, "sname" : entry.filename, "lname": entry.filename, "dtmodified" : datetime.fromtimestamp(entry.st_mtime)})
                    else :
                        files.append({"src":source, "sname" : entry.filename, "lname": entry.filename, "dtmodified" : datetime.fromtimestamp(entry.st_mtime)})
            return files
        else:
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
    
    def downloadfile(self, source, destination, sfilename, lfilename, retainfolderstructure=False, overridefile = False, preserve_mtime=False) :
        localpath = destination +"/" + source
        remotepath = source+"/"+sfilename
        if self.sftp.exists(remotepath):
            if not os.path.exists(destination):
                os.makedirs(destination)
            if retainfolderstructure:
                if not os.path.exists(localpath) :
                    os.makedirs(localpath, exist_ok=True)
                localpath =  localpath + "/" + sfilename
            else:
                localpath = destination + "/" + lfilename
            if not overridefile :
                if not os.path.exists(localpath):
                    self.sftp.get(remotepath, localpath, preserve_mtime=preserve_mtime)
            else:
                self.sftp.get(remotepath, localpath, preserve_mtime=preserve_mtime)
        else :
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
        