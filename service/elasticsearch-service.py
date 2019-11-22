from flask import Flask, request, Response
import cherrypy
from datetime import datetime, timedelta
import json
import logging
import paste.translogger
import requests


app = Flask(__name__)

logger = logging.getLogger("elasticsearch-service")

@app.route('/', methods=['GET'])
def root():
    return Response(status=200, response="{ \"status\" : \"OK\" }")


@app.route('/entities', methods=["GET"])
def get():

    logger.info("starting")

    def generate():
        # get data
        is_more = True
        is_first = True
        yield "["
        start_from = 0
        page_size = 1000

        while is_more:
            r = requests.get("http://localhost:9200/_search?size=" + str(page_size) + "&from=" + str(start_from))
            data = r.json()
            print(data)
            if data["took"] == 0:
                is_more = False
            else:
                start_from = start_from + page_size
                hits = data["hits"]["hits"]
                for h in hits:
                    e = h["_source"]
                    e["_id"] = h["_id"]
                    yield json.dumps(e)
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









