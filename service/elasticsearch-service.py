from flask import Flask, request, Response
import cherrypy
from datetime import datetime, timedelta
import json
import logging
import paste.translogger
import requests
import os
import boto3
from botocore.credentials import InstanceMetadataProvider, InstanceMetadataFetcher
from requests_aws4auth import AWS4Auth

secret_key = os.environ.get("SECRET_KEY")
access_key = os.environ.get("ACCESS_KEY")

if secret_key == None:
    logger = logging.getLogger("elasticsearch-service")
    logger.info("No params so attempt get config from machine")
    provider = InstanceMetadataProvider(iam_role_fetcher=InstanceMetadataFetcher(timeout=1000, num_attempts=2))
    credentials = provider.load()

    access_key = credentials.access_key
    secret_key = credentials.secret_key

region = os.environ.get('REGION')
if region == None:
    region = "eu-central-1"

def executeSignedPost(url, body):
    service = 'es'
    awsauth = AWS4Auth(access_key, secret_key, region, service)
    r = requests.post(url, auth=awsauth, json=body)
    result = r.json()
    return result
    
app = Flask(__name__)

logger = logging.getLogger("elasticsearch-service")
index_name = os.environ.get('INDEX')
if index_name != None:
    index_name = "/" + index_name 
else:
    index_name = ""

scroll_keep_alive = os.environ.get('SCROLL_KEEP_ALIVE')
if scroll_keep_alive == None:
    # default to 1 minute
    scroll_keep_alive = "1m"
logger.info(scroll_keep_alive)

endpoint = os.environ.get('ES_ENDPOINT')
if endpoint == None:
    endpoint = "http://localhost:9200"
logger.info(endpoint)

@app.route('/', methods=['GET'])
def root():
    return Response(status=200, response="{ \"status\" : \"OK\" }")

@app.route('/entities', methods=["GET"])
def get():

    logger.info("get entities")

    def generate():
        is_more = True
        is_first = True
        yield "["
        page_size = 1

        # do initial scroll query
        query = {}
        query["query"] = {}
        query["query"]["match_all"] = {}
        query["size"] = page_size
        data = executeSignedPost(endpoint + index_name + "/_search?scroll=" + scroll_keep_alive, query)

        if len(data["hits"]["hits"]) == 0:
            is_more = False

        while is_more:
            hits = data["hits"]["hits"]
            for h in hits:
                e = h["_source"]
                e["_id"] = h["_id"]
                if is_first:
                    is_first = False                        
                else:
                    yield ","
                yield json.dumps(e)

            # get next scroll
            scroll_request = {}
            scroll_request["scroll"] = scroll_keep_alive
            scroll_request["scroll_id"] = data["_scroll_id"]
            data = executeSignedPost(endpoint + "/_search/scroll", scroll_request)
           
            if len(data["hits"]["hits"]) == 0:
                is_more = False
    
        yield "]"

    return Response(generate(), mimetype='application/json', )


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout, change to or add a (Rotating)FileHandler to log to a file
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    # Comment these two lines if you don't want access request logging
    app.wsgi_app = paste.translogger.TransLogger(app.wsgi_app, logger_name=logger.name,
                                                 setup_console_handler=False)
    app.logger.addHandler(stdout_handler)

    logger.propagate = False
    logger.setLevel(logging.INFO)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5000,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()













