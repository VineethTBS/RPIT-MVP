from flask import request 
from flask import jsonify 
from flask import Blueprint
from pfapiutils import api_response
from injector import inject
from processors.sftpprocessor import SftpProcessor
import http.client

api_dataintake = Blueprint("api_dataintake", __name__)

@inject
@api_dataintake.route("/dataintake/<id>/<filtertable>", methods = ['GET'])
def get_dataintake_data_by_id(processor: SftpProcessor, id,filtertable):
  """API will get  details based on the id from the any table of DataIntake
  ----
  tags: 
    - Data Intake
  parameters:
    - name: id
      in: path
      type: string
      required: true
      description: ID 
    - name: filtertable
      in: path
      type: string
      required: true
      enum: [sftpconfig,task]
      description: Filter condition supporting values 'sftpconfig','task'
  responses:
    200:
      description: Get Data Intake details based on the id.
  """
  resp = processor.get_dataintake_id(id,filtertable)
  return api_response.create_response(resp, http.client.OK)

@api_dataintake.route("/sftp/<sftpid>/<source>/<istlsenable>", methods = ['PUT'])
def update_dataintake_data(processor: SftpProcessor,sftpid,source,istlsenable):
  """API will update sftp data details based on the id
  ----
  tags: 
    - Data Intake
  parameters:
    - name: sftpid
      in: path
      type: string
      required: true
      description: ID of sftp
    - name: source
      in: path
      type: string
      required: false
      description: Source of sftp
    - name: istlsenable
      in: path
      type: boolean
      required: false
      description: Istlsenable
  responses:
    200:
      description: Updated SFTP data details based on the id.
  """
  resp=processor.post_sftpdata(sftpid,source,istlsenable)
  return api_response.create_response(resp,http.client.OK)


@api_dataintake.route("/dataintake/<filtertable>", methods = ['GET'])
def get_dataintake_data(processor: SftpProcessor,filtertable):
  """API will get whole data from the selected table
  ----
  tags: 
    - Data Intake
  parameters:
    - name: filtertable
      in: path
      type: string
      required: true
      enum: [sftpconfig,task]
      description: Filter condition supporting values 'sftpconfig','task'
  responses:
    200:
      description: Get whole from selected table.
  """
  resp=processor.get_dataintake_data(filtertable);
  return api_response.create_response(resp,http.client.OK)

@api_dataintake.route("/dataintake/<id>/<filtertable>", methods = ['DELETE'])
def delete_dataintake_data(processor: SftpProcessor,id,filtertable):
  """API will delete  details based on the id from the any table of DataIntake
  ----
  tags: 
    - Data Intake
  parameters:
    - name: id
      in: path
      type: string
      required: true
      description: ID 
    - name: filtertable
      in: path
      type: string
      required: true
      enum: [sftpconfig,task]
      description: Filter condition supporting values 'sftpconfig','task'
  responses:
    200:
      description: Delete data based on id from the any table of DataIntake
  """
  resp = processor.delete_dataintake_data(id,filtertable)
  return api_response.create_response(resp, http.client.OK)

@api_dataintake.route("/sftp/<source>/<istlsenable>/<protocol>/<retainfolderstructure>/<handlefilewithsamename>/<overridefile>", methods = ['POST'])
def create_dataintake_data(processor: SftpProcessor,source,istlsenable,protocol,retainfolderstructure,handlefilewithsamename,overridefile):
  """API will create new  sftp data 
  ----
  tags: 
    - Data Intake
  parameters:
    - name: source
      in: path
      type: string
      required: false
      description: Source of sftp
    - name: retainfolderstructure
      in: path
      type: boolean
      required: false
      description: Rules
    - name: handlefilewithsamename
      in: path
      type: boolean
      required: false
      description: Rules
    - name: overridefile
      in: path
      type: boolean
      required: false
      description: Rules
    - name: istlsenable
      in: path
      type: boolean
      required: false
      description: Istlsenable
    - name: protocol
      in: path
      type: string
      required: false
      description: Protocol will be like 'FTP','SFTP'
  responses:
    200:
      description: Updated SFTP data details based on the id.
  """
  resp=processor.create_sftp_date(source,istlsenable,protocol,retainfolderstructure,handlefilewithsamename,overridefile)
  return api_response.create_response(resp, http.client.OK)