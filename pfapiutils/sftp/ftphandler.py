import ftplib
import ssl
import os, re
from const import errorconst
from datetime import datetime
class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

class FTPTLSSession(ImplicitFTP_TLS):

    def __init__(self, host, user, password, port, timeout):
        ImplicitFTP_TLS.__init__(self)
        self.connect(host, port, timeout)
        self.login(user, password)
        # Set up encrypted data connection.
        self.prot_p()

class FTPSession(ftplib.FTP):
    
    def __init__(self, host, userid, password, port, timeout):
        """Act like ftplib.FTP's constructor but connect to another port."""
        ftplib.FTP.__init__(self)
        self.connect(host, port, timeout)
        self.login(userid, password)
        
class FtpHandler:
    def __init__(self, ftpconnector):
        self.__ftp = ftpconnector.connect()

    #This method returns list of 'file attributes' of file 
    def get_file_attributes(self, src, filter=None, files=None):
        ftp = self.__ftp
        if not files:
            files = []
        if ftp.path.isdir(src):
            for entry in ftp.listdir(src):
                remotepath = src + "/" + entry
                files=self.file_fiter(remotepath,filter,entry,ftp,src)
            return files
        else:
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
    
    def file_fiter(self,remotepath,filter,entry,ftp,src):
        if ftp.path.isdir(remotepath):
            files = self.get_file_attributes(remotepath, filter, files)
        else :
            if filter :
                match = re.search(filter, entry, re.IGNORECASE)
                if match:
                    print(f"matched file is {entry}")
                    files.append({"src":src, "sname" : entry, "lname": entry, 
                                    "dtmodified" :datetime.fromtimestamp(ftp.path.getmtime(remotepath))})
            else :
                files.append({"src":src, "sname" : entry, "lname": entry,
                                "dtmodified" :datetime.fromtimestamp(ftp.path.getmtime(remotepath))})
        return files
      

    def downloadfile(self, src, dest, sfilename, lfilename, retainfolderstructure=False, overridefile = False) :
        ftp = self.__ftp
        localpath = dest +"/" + src
        remotepath = src+"/"+sfilename
        if ftp.path.isdir(src):
            localpath= self.folderstructure(retainfolderstructure,localpath,sfilename,dest,lfilename)
            if not os.path.exists(dest):
                os.makedirs(dest)
            if not overridefile :
                if not os.path.exists(localpath):
                    ftp.download(remotepath, localpath)
            else:
                ftp.download(remotepath, localpath)
        else :
            raise OSError(errorconst.SFTP_NO_DIRECTORY_FOUND)
    
    def folderstructure(self,retainfolderstructure,localpath,sfilename,dest,lfilename):
        if retainfolderstructure:
                if not os.path.exists(localpath) :
                    os.makedirs(localpath, exist_ok=True)
                localpath =  localpath + "/" + sfilename
        else:
                localpath = dest + "/" + lfilename
        return localpath
