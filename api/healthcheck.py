from flask import request,jsonify,Blueprint
from pfapiutils import api_response
from flasgger import Swagger
from flasgger.utils import swag_from

api_healthcheck = Blueprint("api_healthcheck", __name__)

@api_healthcheck.route("/healthcheck/", methods = ['GET'])
def get_healthcheck():
    """Sample GET api for Healthcheck
    Healthcheck GET method
    ----
    responses:
      200:
        description: Returns pong with message code 200
    """
    return "pong"

@api_healthcheck.route("/healthcheck/<string:msg>", methods = ['POST'])
def post_healthcheck(msg = 'hello'):
    """Sample POST api for Healthcheck
     Healthcheck POST method
    ----
    parameters:
      - name: msg
        in: path
        type: string
        required: true
        default: hello
    responses:
      200:
        description: Returns Json response with message code 200
        
    """
    return api_response.create_response(msg)
