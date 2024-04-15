from flask import request 
from flask import jsonify 
from flask import Blueprint
from pfapiutils import api_response
from injector import inject
from processors.deallistprocessor import DealListProcessor
import http.client

api_deallist = Blueprint("api_deallist", __name__)

@inject
@api_deallist.route("/deallist/", methods = ['GET'])
def getdeallist(processor: DealListProcessor):
  """API will get DealList details
  ----
  tags: 
    - Deal List 
  responses:
    200:
      description: Get Deal List details.
  """
  resp = processor.get_deallist_data()
  return api_response.create_response(resp, http.client.OK)