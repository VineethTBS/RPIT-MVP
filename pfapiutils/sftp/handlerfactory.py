from const import errorconst
from pfapiutils.sftp import ftphandler as fh, sftphandler as sh, fsconnector as con

class HandlerFactory :
    
    def get_handler(self, config):
        if str(config["protocol"]).lower() == "sftp" :
            sftp = con.FSConnector(host = config["hostname"], uid = config["uid"], pwd = config["pwd"],
                                   port = config["port"], protocol= config["protocol"],
                                   hostfilepath= config["hostfile"])
            return sh.SftpHandler(sftp)
        elif str(config["protocol"]).lower() == "ftp" :
            ftp = con.FSConnector(host = config["hostname"], uid = config["uid"], pwd = config["pwd"],
                                  port = config["port"], protocol= config["protocol"],
                                  isTLS=config["isTLSenable"])
            return fh.FtpHandler(ftp)
        else :
            raise Exception(errorconst.HANDLER_NOT_AVAILABLE)
            