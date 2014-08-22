#!/usr/bin/python

import flask
import flask.ext.sqlalchemy
import flask.ext.restless
from flask.ext.cors import cross_origin
import os
import sys

sys.path.insert(0, '.')
import database


def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'     ] = '*'
    #response.headers['Access-Control-Allow-Credentials'] = 'true'
    # Set whatever other headers you like...
    return response

#https://flask-restless.readthedocs.org/en/latest/quickstart.html
# Create the Flask application and the Flask-SQLAlchemy object.
app = flask.Flask(__name__)

db_name = 'sqlite:///' + os.path.abspath( sys.argv[1] )
print db_name
app.config['DEBUG'                  ] = True
app.config['SQLALCHEMY_DATABASE_URI'] = db_name
app.config['SERVER_PORT'            ] = 5000

cors = CORS(app, resources={r"/api/*": {"origins": "*"}},
            headers="Content-Type")


app.secret_key = 's3cr3tkeyverysecret'
db = flask.ext.sqlalchemy.SQLAlchemy(app)

# Create the Flask-Restless API manager.
manager = flask.ext.restless.APIManager(app, flask_sqlalchemy_db=db)

# Create API endpoints, which will be available at /api/<tablename> by
# default. Allowed HTTP methods can be specified as well.
for dbn in database.dbs:
	print "exporting", dbn
	blueprint = manager.create_api(dbn, methods=['GET'], allow_functions=True)
	blueprint.after_request(add_cors_headers)

# start the flask loop
app.run(host='0.0.0.0', debug=True, port=5000)
