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
 
  resp = processor.get_available_poolcuts(uniqueidentifiertype.strip(), uniqueidentifier.strip(), poolcutdate, filtercondition)
  return api_response.create_response(resp, http.client.OK)

@inject
@api_dataexport.route("/dataexport/downloadrawdata/<filemetadataid>", methods = ['GET'])
def downloadrawdata(processor: DataexportProcessor, filemetadataid):
 
  filepath = processor.download_rawdata(filemetadataid)
  return api_response.send_file_response(filepath, as_attachment=True)

@inject
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>", methods = ['GET'])
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>", methods = ['GET'])
@api_dataexport.route("/dataexport/downloadrawdata/<uniqueidentifiertype>/<uniqueidentifier>/<poolcutdate>/<filtercondition>", methods = ['GET'])
def download_rawdata_generic(processor: DataexportProcessor, uniqueidentifiertype, uniqueidentifier, poolcutdate = None, filtercondition = 'eq'):

  
  filepath = processor.download_rawdata_generic(uniqueidentifiertype.strip(), uniqueidentifier.strip(), poolcutdate, filtercondition)
  return api_response.send_file_response(filepath, as_attachment=This Flask application code snippet defines a set of HTTP endpoints that provide functionality for exporting data based on metadata or raw data requests. The application uses a combination of Flask, a lightweight WSGI web application framework, and dependency injection for managing dependencies. Let's break down the code, its structure, and flow in detail:

### Imports and Setup

1. **Flask Imports:** The code imports `Blueprint` for organizing the application into components, `request` for handling incoming requests, and `jsonify` for creating JSON responses.
   
2. **Custom Utility Import:** `api_response` from `pfapiutils` module is imported for creating standardized API responses.

3. **Dependency Injection Import:** `inject` decorator from the `injector` module is used to inject dependencies into the route functions automatically.

4. **Business Logic Import:** `DataexportProcessor` from `processors.dataexportprocessor` module contains the business logic for data export functionalities.

5. **HTTP Status Codes:** `http.client` module is imported for using HTTP status codes for responses, ensuring readability and maintenance of standard practices.

### Blueprint Configuration

- **Blueprint Creation:** A Blueprint named `api_dataexport` is created to group all data export related routes. This allows for modular architecture and easier management of the application's components.

### Route Definitions and Functions

1. **Metadata Export Endpoints:**
   - These endpoints are defined for fetching metadata based on a unique identifier, an optional pool cut date, and an optional filter condition. They support GET requests and have three variations to accommodate different parameters.
   - The route decorator `@api_dataexport.route` is used to map URLs to the `get_available_poolcuts` function. This function takes a `DataexportProcessor` instance (injected by the `@inject` decorator) and the URL parameters as arguments.
   - The `get_available_poolcuts` function calls the `processor.get_available_poolcuts` method with the provided parameters, and returns a standardized response using the `api_response.create_response` function, along with an HTTP OK status code.

2. **Raw Data Download Endpoints:**
   - There are two sets of endpoints for downloading raw data. The first set allows downloading by a specific `filemetadataid`, and the second set supports downloading by unique identifier types, with optional parameters for the pool cut date and filter condition.
   - Similar to the metadata export endpoints, these routes use the `@api_dataexport.route` decorator for URL mapping and the `@inject` decorator for dependency injection.
   - The `downloadrawdata` and `download_rawdata_generic` functions handle the logic for fetching the file path of the raw data to be downloaded. They call respective methods on the `DataexportProcessor` instance and then use the `api_response.send_file_response` function to return the file as an attachment to the client.

### Code Flow Summary

- The application defines a set of endpoints for exporting data based on metadata or raw data requests. 
- It uses Flask's Blueprint for modularity, dependency injection for cleaner code, and a separation of concerns where the `DataexportProcessor` handles the business logic.
- Requests are mapped to functions via routes, which process the input parameters, interact with the business logic layer, and finally return responses in a standardized format or as file downloads.

This structure facilitates easy maintenance, scalability, and separation of concerns between the web layer and business logic, adhering to good software development practices.
