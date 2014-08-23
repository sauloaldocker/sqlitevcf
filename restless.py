#!/usr/bin/python

import os
import sys
from collections import defaultdict

import flask
import flask.ext.sqlalchemy
import flask.ext.restless
from flask.ext.cors import CORS
from flask          import jsonify

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

cors = CORS(app, resources={r"/*": {"origins": "*"}},
            headers="Content-Type")


app.secret_key = 's3cr3tkeyverysecret'
db = flask.ext.sqlalchemy.SQLAlchemy(app)

# Create the Flask-Restless API manager.
manager = flask.ext.restless.APIManager(app, flask_sqlalchemy_db=db)

# Create API endpoints, which will be available at /api/<tablename> by
# default. Allowed HTTP methods can be specified as well.
databases_meta = defaultdict(list)

#class ChromGrp(database.Chroms):
#	pass
#database.dbs = tuple( list(database.dbs) + [ChromGrp] )

for dbn in database.dbs:
	print "exporting", dbn
	#manager.create_api(dbn, methods=['GET'], allow_functions=True, collection_name=dbn.__tablename__)
	manager.create_api(dbn, methods=['GET'], allow_functions=True)

	if len( databases_meta ) == 0:
		tables = dbn.metadata.sorted_tables
		for table in tables:
			#print t
			for column in table.c:
				#print c
				#print dir(c)
				#print " anon_label  ", column.anon_label
				#print " base_columns", column.base_columns
				#print " desc        ", column.desc
				#print " description ", column.description
				#print " info        ", column.info
				#print " key         ", column.key
				#print " label       ", column.label
				#print " name        ", column.name
				#print " table       ", column.table
				#print " type        ", column.type
				#name         format_str
				#table        format_col
				#type         VARCHAR
				databases_meta[ str(column.table) ].append( ( column.name, str(column.type) ) )

			#print dir(t)
			#print "columns", table.columns
			#print "desc   ", table.description
			#print "info   ", table.info
			#print "meta   ", table.metadata
			#print "named  ", table.named_with_column
			#print "schema ", table.schema
	        	#databases_met[ t ] = 
			#break
		#databases_met[0] = 1

#print "metadata ", databases_meta

@app.route('/')
def hello_world():
    hlp = { 
	'/'             : 'help', 
	'/metas'        : 'all tables meta data',
        '/meta'         : 'list of tables',
        '/meta/<CHROM>' : 'metadata of table <CHROM>',
        '/api/<TABLE>'  : 'api for table <TABLE>'
    }
    return jsonify( { 'help': hlp } )

@app.route('/metas')
def metas():
    return jsonify( { 'metas': databases_meta } )

@app.route('/meta')
def meta():
    names = databases_meta.keys()
    names.sort()
    print names
    return jsonify( { 'meta': names } )

@app.route('/meta/<table>')
def meta_table(table):
    if table not in databases_meta:
        flask.abort(404)
    dbn_meta = databases_meta[ table ]
    return jsonify({ 'meta': dbn_meta })

# start the flask loop
app.run(host='0.0.0.0', debug=True, port=5000)
