# VERSION        0.1
# DOCKER-VERSION 0.10.0
# AUTHOR         Saulo Alves <sauloal@gmail.com>
# DESCRIPTION
# TO BUILD       docker build -t sauloal/vcflite .
# TO UPLOAD      while true; do docker push sauloal/vcflite; echo $?; if [ $? == "0" ]; then exit 0; fi; done
#
#
# TO RUN docker run -i -t --rm -v $PWD/data/:/data:rw -v $PWD/db:/db                                   --name="sauloal_vcflite_add" sauloal/vcflite sqlite_vcf.py /db/db.sqlite /data/*.vcf.gz
# TO RUN docker run -i -t --rm -v $PWD/data/:/data:rw -v $PWD/db:/db --link=sauloal_vcflite_api:sqlite --name="sauloal_vcflite_qry" sauloal/vcflite query.py
# TO RUN docker run -d         -v $PWD/data/:/data:rw -v $PWD/db:/db -p 0.0.0.0:5000:5000              --name="sauloal_vcflite_api" sauloal/vcflite api.py        /db/db.sqlite
# TO RUN docker run -d         -v $PWD/data/:/data:rw -v $PWD/db:/db -p 0.0.0.0:5000:5000              --name="sauloal_vcflite_api" sauloal/vcflite restless.py   /db/db.sqlite
#
#
# run in interactive mode, defines the database name, delete after run, link data folder to internal data folder, link to mongodb server
#

FROM sauloal/ubuntu14.04

MAINTAINER Saulo Alves <sauloal@gmail.com>

ENV DEBIAN_FRONTEND    noninteractive
ENV DEBIAN_PRIORITY    critical
ENV DEBCONF_NOWARNINGS yes

RUN	apt-get update; \
	apt-get install -y python-setuptools build-essential git python-dev; \
	apt-get clean all

ENV PYTHON_EGG_DIR /tmp

RUN easy_install -Z pip pyvcf sqlalchemy sandman requests Flask-Restless flask-cors

RUN cd /opt; \
    wget https://bitbucket.org/pypy/pypy/downloads/pypy-2.3.1-linux64.tar.bz2; \
    tar xvf pypy-2.3.1-linux64.tar.bz2; \
    rm pypy-2.3.1-linux64.tar.bz2; \
    ln -s $PWD/pypy-2.3.1-linux64/bin/pypy /usr/bin/; \
    cd /opt/pypy-2.3.1-linux64/site-packages; \
    cp -R /usr/local/lib/python2.7/dist-packages/* .



VOLUME /data
VOLUME /db

WORKDIR /

ADD api.py        /bin/api.py
ADD restless.py   /bin/restless.py
ADD database.py   /bin/database.py
ADD sqlite_vcf.py /bin/sqlite_vcf.py
ADD query.py      /bin/query.py

EXPOSE 5000
