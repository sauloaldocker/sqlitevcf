#!/usr/bin/python

import requests
import json
import os
from   pprint import pprint as pp
#https://flask-restless.readthedocs.org/en/latest/searchformat.html


#SQLITE_PORT=tcp://172.17.0.57:5000
#SQLITE_PORT_5000_TCP=tcp://172.17.0.57:5000
#SQLITE_PORT_5000_TCP_ADDR=172.17.0.57
#SQLITE_PORT_5000_TCP_PORT=5000
#SQLITE_PORT_5000_TCP_PROTO=tcp

PORT=os.environ.get( 'SQLITE_PORT_5000_TCP_PORT',        5000 )
IPAD=os.environ.get( 'SQLITE_PORT_5000_TCP_ADDR', '127.0.0.1' )


queries = [
	[ 'chrom', dict(results_per_page=15), [dict(name='chrom_name', op='like', val='SL2.40%')], [] ]
]


for DBNA, url_params, filters, extras in queries:
	url           = 'http://%s:%d/api/%s' % ( IPAD, int(PORT), DBNA )
	print "URL", url

	headers  = {'Content-Type': 'application/json'}
	req      = dict( filters=filters )

	for extra in extras:
		for e in extra:
			req[ e ] = extra[ e ]

	pp( req )

	params   = dict( q=json.dumps( req ) )
	for p in url_params:
		params[ p ] = url_params[ p ]

	pp( params )

	response = requests.get(url, params=params, headers=headers)

	print "RESPONSE URL        ", response.url
	print "RESPONSE STATUS CODE", response.status_code
	print "RESPONSE HEADERS    ", response.headers
	print "RESPONSE ENCODING   ", response.encoding
	print "RESPONSE TEXT\n", response.text
	#print( response.json() )
	#assert response.status_code == 200

#curl   -G   -H "Content-type: application/json"   -d "q={\"filters\":[{\"name\":\"chrom_name\",\"op\":\"like\",\"val\":\"SL2.0ch0%\"}]}"   http://127.0.0.1:5000/api/chrom

#results_per_page

#https://flask-restless.readthedocs.org/en/latest/searchformat.html#operators
#Operators
#The operator strings recognized by the API incude:
#==, eq, equals, equals_to

#!=, neq, does_not_equal, not_equal_to
#>, gt, <, lt
#>=, ge, gte, geq, <=, le, lte, leq
#in, not_in
#is_null, is_not_null
#like
#has
#any

#,"disjunction":true > satisfy any of filters
#,"single":true > error if not only one result
#[{"name":"age","op":"ge","field":"height"}]
#GET /api/person?q={"filters":[{"name":"computers","op":"any","val":{"name":"id","op":"gt","val":1}}]} HTTP/1.1
#Host: example.com
#the response will include only those Person instances that have a related Computer instance with id field of value greater than 1:
#limit
#A positive integer which specifies the maximum number of objects to return.
#offset
#A positive integer which specifies the offset into the result set of the returned list of instances.
#order_by

#GET /api/eval/person?q={"functions": [{"name": "sum", "field": "age"}, {"name": "avg", "field": "height"}]} HTTP/1.1
#The format of the response is
#HTTP/1.1 200 OK
#{"sum__age": 100, "avg_height": 68}

#GET /api/eval/person?q={"functions": [{"name": "count", "field": "id"}]} HTTP/1.1
#Response:
#HTTP/1.1 200 OK
#{"count__id": 5}
