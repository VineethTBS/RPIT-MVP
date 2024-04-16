import pysftp
import ftputil
import ftputil.session
from const import errorconst
import traceback
from pfapiutils.sftp import ftphandler


class FSConnector() :
    
    def __init__(self, host, uid, pwd, port = None, protocol="ftp", isTLS = True, timeout=30, hostfilepath=None):
        self.__host = host
        self.__uid = uid
        self.__pwd = pwd
        self.__port = port
        self.__isTLS = isTLS
        self.__timeout = timeout
        self.__protocol = protocol
        self.__cnopts = pysftp.CnOpts()
        if hostfilepath :
            self.__cnopts.hostkeys.load(hostfilepath)
        else:
            self.__cnopts.hostkeys = None
    
    def connect(self):
        
        if self.__protocol.lower() == "ftp":
            self.__port = 21 if self.__port is None else self.__port
            return self.__connectftp()
        elif self.__protocol.lower() == "sftp":
            self.__port = 22 if self.__port is None else self.__port
            return self.__connectsftp()
        else:
            raise Exception(errorconst.PROTOCOL_NOT_SUPPORTED)
            
    def __connectsftp(self):
        try:            
            return pysftp.Connection(host=self.__host, username=self.__uid, password=self.__pwd, port=self.__port, cnopts = self.__cnopts)
        except Exception:
            #TODO : log exception
            raise Exception(f"{errorconst.SFTP_CONNECTION_FAILED}{traceback.format_exc()}")
        
    def __connectftp(self):
        try:
            return ftputil.FTPHost(self.__host, self.__uid, self.__pwd, self.__port, self.__timeout, session_factory=ftphandler.FTPTLSSession if self.__isTLS else ftphandler.FTPSession)
        except Exception:
            #TODO : log exception
            raise Exception(f"{errorconst.SFTP_CONNECTION_FAILED}{traceback.format_exc()}")
