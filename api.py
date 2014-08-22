#!/usr/bin/python

#http://www.jeffknupp.com/blog/2013/07/23/sandman-a-boilerplatefree-python-rest-api-for-existing-databases/
#https://sandman.readthedocs.org/en/latest/using_sandman.html

import os
import sys

sys.path.insert(0, '.')
import database

from sandman       import app, db

db_name = 'sqlite:///' + os.path.abspath( sys.argv[1] )
print db_name
app.config['SQLALCHEMY_DATABASE_URI'] = db_name
app.config['SANDMAN_SHOW_PKS'       ] = False
app.config['SANDMAN_GENERATE_PKS'   ] = True
app.config['SERVER_PORT'            ] = 5000
app.secret_key = 's3cr3tkeyverysecret'
#model.activate_admin_classes = True

import sandman
from sandman.model import register, Model
from sandman.model import activate
print sandman.__version__

register(database.dbs, use_admin=True)
#register( ( Chroms ) )

activate(browser=False, reflect_all=True)
#activate(host='0.0.0.0', browser=True, reflect_all=True)

#app.run()
app.run(host='0.0.0.0', debug=True)
