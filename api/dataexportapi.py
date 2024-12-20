from flask import request # to handle http requests and create json responses 
from flask import jsonify #
from flask import Blueprint
from pfapiutils import api_response #for creating standridze API response .
from injector import inject # 
from processors.dataexportprocessor import DataexportProcessor 
import http.client

api_dataexport = Blueprint("api_dataexport", __name__)
    
@inject
@api_dataexport.route("/dataexport/metadata/<uniqueidentifiertype>/<uniqueidentifier>", methods = ['GET'])
@api_dataexport.route("/dataexport/metadata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>", methods = ['GET'])
@api_dataexport.route("/dataexport/metadata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>/<filtercondition>", methods = ['GET'])
def get_available_poolcuts(processor: DataexportProcessor, uniqueidentifiertype, uniqueidentifier, poolcutdate = None, filtercondition = 'eq'):
  """API will return details of poolcut(s) from specified date 
  ----
    tags: 
      - data export
    parameters:
      - name: uniqueidentifiertype
        in: path
        type: string
        required: true
        description: Name of Unique Identifier (Ex. EDCode, DealId, PoolId etc.)
      - name: uniqueidentifier
        in: path
        type: string
        required: true
        description: Unique Identifier's value
      - name: poolcutdate
        in: path
        type: string
        format: string
        required: false
        description: Poolcut date in 'YYYY-MM-DD' format
      - name: filtercondition
        in: path
        type: string
        format: string
        required: false
        enum: [gt, lt, gte, lte, eq]
        description: Filter condition supporting values 'gt', 'lt', 'gte', 'lte', 'eq'
    responses:
      200:
        description: Returns available poolcut(s) details
  """
  
  resp = processor.get_available_poolcuts(uniqueidentifiertype.strip(), uniqueidentifier.strip(), poolcutdate, filtercondition)
  return api_response.create_response(resp, http.client.OK)

@inject
@api_dataexport.route("/dataexport/downloadrawdata/<filemetadataid>", methods = ['GET'])
def downloadrawdata(processor: DataexportProcessor, filemetadataid):
  """API will download RAW data in Excel file
  ----
  tags: 
    - data export
  parameters:
    - name: filemetadataid
      in: path
      type: string
      required: true
      description: ID of File Metadata
  responses:
    200:
      description: Downloads RAW data in CSV file.
  """
  filepath = processor.download_rawdata(filemetadataid)
  return api_response.send_file_response(filepath, as_attachment=True)

@inject
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>", methods = ['GET'])
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>", methods = ['GET'])
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>/<filtercondition>", methods = ['GET'])
def download_rawdata_generic(processor: DataexportProcessor, uniqueidentifiertype, uniqueidentifier, poolcutdate = None, filtercondition = 'eq'):
  """API will return details of poolcut(s) from specified date 
  ----
    tags: 
      - data export
    parameters:
      - name: uniqueidentifiertype
        in: path
        type: string
        required: true
        description: Name of Unique Identifier (Ex. EDCode, DealId, PoolId etc.)
      - name: uniqueidentifier
        in: path
        type: string
        required: true
        description: Unique Identifier's value
      - name: poolcutdate
        in: path
        type: string
        format: string
        required: false
        description: Poolcut date in 'YYYY-MM-DD' format
      - name: filtercondition
        in: path
        type: string
        format: string
        required: false
        enum: [gt, lt, gte, lte, eq]
        description: Filter condition supporting values 'gt', 'lt', 'gte', 'lte', 'eq'
    responses:
      200:
        description: Downloads RAW data in CSV file.
  """
  
  filepath = processor.download_rawdata_generic(uniqueidentifiertype.strip(), uniqueidentifier.strip(), poolcutdate, filtercondition)
  return api_response.send_file_response(filepath, as_attachment=True)
