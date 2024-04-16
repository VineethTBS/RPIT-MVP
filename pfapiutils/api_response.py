from flask import make_response, send_file

def create_response(result, code = 200, error=None):
    return make_response({"data":result, "error":error, "statuscode": code}, code)

def send_file_response(filename_or_fp, mime_type=None, as_attachment=False, attachment_filename=None):
    return send_file(filename_or_fp, mimetype=mime_type, as_attachment=as_attachment, 
                     attachment_filename=attachment_filename)