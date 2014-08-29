#!/usr/bin/python
import os
import sys
import inspect
from collections    import namedtuple

print "importing sqlalchemy"
import sqlalchemy

print "importing sqlalchemy libs"
from sqlalchemy        import create_engine
from sqlalchemy        import Column, Integer, String, Float, Boolean, BigInteger
from sqlalchemy        import ForeignKey, Sequence, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy.sql    import text
from sqlalchemy.engine import reflection
from sqlalchemy        import Index, MetaData

print "importing sqlalchemy declarative"
from sqlalchemy.ext.declarative import declarative_base

print "importing sqlalchemy orm"
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm import relationship, backref


print 'SQLALCHEMY VERSION', sqlalchemy.__version__


if 'MYSQL_ENV_MYSQL_ROOT_PASSWORD' in os.environ:
    os.environ['MYSQL_PASS'] = os.environ['MYSQL_ENV_MYSQL_ROOT_PASSWORD']

if 'MYSQL_PORT_3306_TCP_ADDR'      in os.environ:
    os.environ['MYSQL_HOST'] = os.environ['MYSQL_PORT_3306_TCP_ADDR'     ]

if 'MYSQL_PORT_3306_TCP_PORT'      in os.environ:
    os.environ['MYSQL_PORT'] = os.environ['MYSQL_PORT_3306_TCP_PORT'     ]

os.environ['MYSQL_SOCK'] = '/tmp/mysql.sock'



#print sorted(os.environ.items())

#docker run -d --name wordpress -v $PWD/mysql/:/var/lib/mysql -e MYSQL_ROOT_PASSWORD="mypass" -p 127.0.1.1:8088:80 -p 127.0.1.1:33060:3306 tutum/wordpress

Base    = declarative_base()

def main(args, echo=True):
    #dbname   = 'test.sqlite'
    #dbname   = 'sqlite:///:memory:'
    dbname   = args[0]
    if os.path.exists( dbname ):
        os.remove( dbname )
    loaddb(dbname, echo=echo)


class IndexBase(Base):
    __abstract__ = True
    metadata = MetaData()

class MetaBase(Base):
    __abstract__ = True
    metadata = MetaData()

class DataBase(Base):
    __abstract__ = True
    metadata = MetaData()


class loaddb(object):
    def __init__(self, db_data_name=None, db_index_name=None, db_meta_name=None, dbtype='SQLITE', echo=False, synchronous=False, inmemory=True, sql_user=None, sql_pass=None, sql_host=None, sql_port=None, sql_sock=None):
        print "creating engine", db_data_name

        #http://stackoverflow.com/questions/8831568/sqlalchemy-declarative-model-with-multiple-database-sharding

        self.dbtype        = dbtype
        self.db_data_name  = db_data_name
        self.db_index_name = db_index_name
        self.db_meta_name  = db_meta_name
        self.db_data       = None
        self.db_index      = None
        self.db_meta       = None

        if self.db_data_name:
            self.db_data  = db_controller( db_data_name , DataBase , dbtype='SQLITE', echo=echo, synchronous=synchronous, inmemory=inmemory, sql_user=sql_user, sql_pass=sql_pass, sql_host=sql_host, sql_port=sql_port, sql_sock=sql_sock )

        if self.db_index_name:
            self.db_index = db_controller( db_index_name, IndexBase, dbtype='SQLITE', echo=echo, synchronous=synchronous, inmemory=inmemory, sql_user=sql_user, sql_pass=sql_pass, sql_host=sql_host, sql_port=sql_port, sql_sock=sql_sock )

        if self.db_meta_name:
            self.db_meta  = db_controller( db_meta_name , MetaBase , dbtype='SQLITE', echo=echo, synchronous=synchronous, inmemory=inmemory, sql_user=sql_user, sql_pass=sql_pass, sql_host=sql_host, sql_port=sql_port, sql_sock=sql_sock )

        self.dbs = {
            'data' : self.db_data,
            'index': self.db_index,
            'meta' : self.db_meta
        }

        print "finished"

    def list_dbs(self):
        return sorted([ x for x in self.dbs.keys() if self.dbs[x] is not None ])

    def get_dbs(self):
        dbkeys = self.list_dbs
        res    = namedtuple(*dbkeys)
        return res( [ self.dbs[x] for x in dbkeys ])

    def get_db(self, dbname):
        db = self.dbs.get(dbname, None)
        if db:
            return db

    def get_engines(self):
        dbkeys = self.list_dbs
        res    = namedtuple(*dbkeys)
        return res( [ self.dbs[x].get_engine() for x in dbkeys ])

    def get_engine(self, dbname):
        db = self.dbs.get(dbname, None)
        if db:
            return db.get_engine()

    def get_sessions(self):
        dbkeys = self.list_dbs
        res    = namedtuple(*dbkeys)
        return res( [ self.dbs[x].get_session() for x in dbkeys ])

    def get_session(self, dbname):
        db = self.dbs.get(dbname, None)
        if db:
            return db.get_session()

    def get_or_update_data(self, dbname, cls, att, val):
        db = self.dbs.get(dbname, None)
        if db:
            return db.get_or_update(cls, att, val)

    def list_indexes(self, dbname, table_name=None):
        db = self.dbs.get(dbname, None)
        if db:
            return db.list_indexes(table_name=table_name)

    def drop_indexes(self, dbname, table_name=None):
        db = self.dbs.get(dbname, None)
        if db:
            return db.drop_indexes(table_name=table_name)

    def add_indexes(self, dbname, indexes, table_name=None):
        db = self.dbs.get(dbname, None)
        if db:
            return db.add_indexes(indexes, table_name=table_name)

    def use(self, dbname):
        db = self.dbs.get(dbname, None)
        if db:
            return db.use()

class db_controller(object):
    def __init__(self, db_name, base, dbtype='SQLITE', echo=False, synchronous=False, inmemory=True, sql_user=None, sql_pass=None, sql_host=None, sql_port=None, sql_sock=None):
        self.cache   = {}
        self.dbtype  = dbtype
        self.db_name = db_name
        self.Base    = base

        if self.dbtype == 'MYSQL':
            MYSQL_NAME = os.environ['MYSQL_NAME'] if 'MYSQL_NAME' in os.environ else None
            MYSQL_USER = os.environ['MYSQL_USER'] if 'MYSQL_USER' in os.environ else 'root'
            MYSQL_PASS = os.environ['MYSQL_PASS'] if 'MYSQL_PASS' in os.environ else None
            MYSQL_HOST = os.environ['MYSQL_HOST'] if 'MYSQL_HOST' in os.environ else '127.0.0.1'
            MYSQL_PORT = os.environ['MYSQL_PORT'] if 'MYSQL_PORT' in os.environ else '3306'
            MYSQL_SOCK = os.environ['MYSQL_SOCK'] if 'MYSQL_SOCK' in os.environ else None

            if sql_user:
                MYSQL_USER = sql_user
            if sql_pass:
                MYSQL_PASS = sql_pass
            if sql_host:
                MYSQL_HOST = sql_host
            if sql_port:
                MYSQL_PORT = sql_port
            if sql_sock:
                MYSQL_SOCK = sql_sock

            if MYSQL_SOCK:
                self.sql_add = 'mysql:///?unix_socket=%(mysql_sock)s' % {
                    'mysql_user': str(MYSQL_USER),
                    'mysql_pass': str(MYSQL_PASS),
                    'mysql_host': '127.0.0.1',
                    'mysql_sock': MYSQL_SOCK,
                    'mysql_db'  : self.db_name }

                print "sql add", self.sql_add

                if not all([MYSQL_USER, MYSQL_PASS]):
                    print "mysql not fully configured"
                    sys.exit(1)

            else:
                if MYSQL_NAME:
                    self.db_name = MYSQL_NAME + '/' + self.db_name

                self.sql_add = 'mysql://%(mysql_user)s:%(mysql_pass)s@%(mysql_host)s:%(mysql_port)s/%(mysql_db)s' % {
                    'mysql_user': str(MYSQL_USER),
                    'mysql_pass': str(MYSQL_PASS),
                    'mysql_host': str(MYSQL_HOST),
                    'mysql_port': str(MYSQL_PORT),
                    'mysql_db'  : self.db_name }

                print "sql add", self.sql_add

                if not all([MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_PORT]):
                    print "mysql not fully configured"
                    sys.exit(1)


        elif self.dbtype == 'SQLITE':
            self.sql_add  = 'sqlite:///' + self.db_name

        else:
            print "unknown database", self.dbtype
            sys.exit(1)




        print "creating session"
        self.engine  = create_engine(self.sql_add, echo=echo)
        self.Session = scoped_session( sessionmaker(bind=self.engine, autocommit=False) )#, autoflush=False, expire_on_commit=False)
        self.session = self.Session()



        if self.sql_add == 'sqlite:///:memory:' or self.dbtype == 'MYSQL' or not os.path.exists(self.db_name):
            if self.dbtype == 'MYSQL':
                try:
                    self.engine.execute("DROP DATABASE " + self.db_name) #create db
                except:
                    pass
                print "CREATING DATABASE"
                self.engine.execute("CREATE DATABASE " + self.db_name) #create db
                print "SELECTING DATABASE"
                self.engine.execute("USE " + self.db_name) # select new db

            print "creating database"
            self.Base.metadata.create_all( self.engine )




        if self.dbtype == 'MYSQL':
            print "SELECTING DATABASE"
            self.session.execute("USE " + self.db_name) # select new db


        if self.dbtype == 'SQLITE':
            if not synchronous:
                self.engine.execute("PRAGMA synchronous = OFF")

            if inmemory:
                #self.session.execute("PRAGMA journal_mode = OFF")
                #self.session.execute("PRAGMA journal_mode = WAL")
                self.engine.execute("PRAGMA journal_mode = MEMORY")

            #self.session.execute("PRAGMA locking_mode = EXCLUSIVE;" )
            self.session.execute("PRAGMA temp_store = MEMORY;"       )
            self.session.execute("PRAGMA count_changes = OFF;"       )
            self.session.execute("PRAGMA PAGE_SIZE = 40960;"         )
            self.session.execute("PRAGMA default_cache_size=7000000;")
            self.session.execute("PRAGMA cache_size=7000000;"        )
            self.session.execute("PRAGMA compile_options;"           )

            self.meta    = MetaData()
            self.meta.reflect(bind=self.engine)
            self.insp    = reflection.Inspector.from_engine( self.engine )

    def use(self):
        if self.dbtype == 'MYSQL':
            self.session.execute("USE " + self.db_name) # select new db

    def get_session(self):
        self.use()
        return self.session

    def get_engine(self):
        return self.engine

    def get_or_update(self, cls, att, val):
        if cls not in self.cache:
            self.cache[ cls ] = {}

        ccache = self.cache[ cls ]
        ckey   = (att, val)
        res    = ccache.get( ckey, None )

        if res is None:
            #print "get_or_update", att, val, 'NOT IN CACHE'
            res = self.session.query( cls ).filter(getattr(cls, att) == val).first()
            if res is None:
                #print "get_or_update", att, val, 'NOT IN CACHE', 'NOT IN DB'
                res = cls(**{att: val})
                self.session.add( res )
                self.session.commit()
                self.session.flush()
            ccache[ ckey ] = res

        else:
            #print "get_or_update", att, val, 'IN CACHE'
            pass

        return res

    def list_indexes(self, table_name=None):
        #http://stackoverflow.com/questions/5605019/listing-indices-using-sqlalchemy
        indexes = {}

        for name in sorted(self.insp.get_table_names()):
            if table_name is not None and table_name != name:
                continue

            if name not in indexes:
                indexes[ name ] = []

            for index in sorted(self.insp.get_indexes(name)):
                print name, index
                indexes[ name ].append( index )

        return indexes

    def drop_indexes(self, table_name=None):
        indexes = self.list_indexes(table_name=table_name)
        for tbl in indexes:
            for nfo in indexes[ tbl ]:
                self.drop_index( tbl, nfo )

    def drop_index(self, table_name, info):
        #coords {'unique': 0, 'name': u'ix_coords_info_FQ', 'column_names': [u'info_FQ']}
        id_name    = info['name'        ]
        id_columns = info['column_names']

        print "droping index", id_name

        Index(id_name).drop( self.engine )

    def add_indexes(self, indexes, table_name=None):
        for tbl in sorted(indexes):
            if table_name is not None and table_name != tbl:
                continue

            for nfo in sorted(indexes[ tbl ]):
                self.add_index( tbl, nfo )

    def add_index(self, table_name, info ):
        id_name    = info['name'        ]
        id_columns = info['column_names']

        print "adding index", id_name

        table   = self.meta.tables[table_name]
        columns = table.columns
        colsO = []
        for coln in id_columns:
            col  = columns[ coln ]
            colsO.append( col )
        Index(id_name, *colsO).create( self.engine )





class Files(IndexBase):
    __tablename__  = 'files'
    __table_args__ = {'sqlite_autoincrement': True}

    FILEPATH_SIZE                 = 1024
    FILEBASE_SIZE                 =  512
    FILENAME_SIZE                 =  256

    file_ID        = Column(Integer, Sequence('files_id'), primary_key=True, autoincrement=True )
    file_path      = Column(String(FILEPATH_SIZE))
    file_base      = Column(String(FILEBASE_SIZE))
    file_name      = Column(String(FILENAME_SIZE))

    #-FILES:
    #--file_ID[AUTOINC]
    #--file_base[STR]
    #--file_name[STR]
    #--file_path[STR]$

    def __repr__(self):
        return "<Files(file_ID='%s', file_path='%s', file_base='%s', file_name='%s')>" % \
        ( str(self.file_ID), self.file_path, self.file_base, self.file_name )

class Header(IndexBase):
    __tablename__  = 'header'
    __table_args__ = {'sqlite_autoincrement': True}

    HEADER_NAME_SIZE              =   32
    HEADER_VALUE_SIZE             =  512

    header_ID      = Column(Integer, Sequence('header_id'), primary_key=True, autoincrement=True )
    header_name    = Column(String(HEADER_NAME_SIZE ) , index=True )
    header_value   = Column(String(HEADER_VALUE_SIZE)              )
    file_ID        = Column(Integer, ForeignKey('files.file_ID'))
    file_src       = relationship("Files", backref=backref('headers', order_by=header_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header(file_ID='%s', header_ID='%s', header_name='%s', header_value='%s')>" % \
                (str( self.file_ID ), str( self.header_ID ),
                 self.header_name, self.header_value)

class Header_format(IndexBase):
    __tablename__      = 'header_format'
    __table_args__     = {'sqlite_autoincrement': True}

    HEADER_FORMAT_NAME_SIZE       =   32
    HEADER_FORMAT_TYPE_SIZE       =   16
    HEADER_FORMAT_DESC_SIZE       =  256

    header_format_ID   = Column(Integer, Sequence('header_format_id'), primary_key=True, autoincrement=True )
    header_format_num  = Column(Integer                         )
    header_format_name = Column(String(HEADER_FORMAT_NAME_SIZE) , index=True )
    header_format_type = Column(String(HEADER_FORMAT_TYPE_SIZE) )
    header_format_desc = Column(String(HEADER_FORMAT_DESC_SIZE) )
    file_ID            = Column(Integer, ForeignKey('files.file_ID'))
    file_src           = relationship("Files", backref=backref('header_formats', order_by=header_format_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_format_ID='%s', header_format_name='%s', header_format_num='%d', header_format_type='%s', header_format_desc='%s')>" % \
        (str(self.file_ID), str(self.header_format_ID),
         self.header_format_name, self.header_format_num, self.header_format_type, self.header_format_desc)

class Header_info(IndexBase):
    __tablename__    = 'header_info'
    __table_args__   = {'sqlite_autoincrement': True}

    HEADER_INFO_NAME_SIZE         =   16
    HEADER_INFO_TYPE_SIZE         =   16
    HEADER_INFO_DESC_SIZE         =   64

    header_info_ID   = Column(Integer, Sequence('header_info_id'), primary_key=True, autoincrement=True )
    header_info_num  = Column(Integer                      )
    header_info_name = Column(String(HEADER_INFO_NAME_SIZE), index=True )
    header_info_type = Column(String(HEADER_INFO_TYPE_SIZE))
    header_info_desc = Column(String(HEADER_INFO_DESC_SIZE))
    file_ID          = Column(Integer, ForeignKey('files.file_ID'))
    file_src         = relationship("Files", backref=backref('header_infos', order_by=header_info_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_info_ID='%s', header_info_name='%s', header_info_num='%d', header_info_type='%s', header_info_desc='%s')>" % \
        (str(self.file_ID), str(self.header_info_ID), self.header_info_name, self.header_info_num, self.header_info_type, self.header_info_desc)

class Header_meta(IndexBase):
    __tablename__    = 'header_meta'
    __table_args__   = {'sqlite_autoincrement': True}

    HEADER_META_NAME_SIZE         =   32
    HEADER_META_DESC_SIZE         =  512

    header_meta_ID   = Column(Integer, Sequence('header_meta_id'), primary_key=True, autoincrement=True )
    header_meta_name = Column(String(HEADER_META_NAME_SIZE) , index=True )
    header_meta_desc = Column(String(HEADER_META_DESC_SIZE)              )
    file_ID          = Column(Integer, ForeignKey('files.file_ID'))
    file_src         = relationship("Files", backref=backref('header_metas', order_by=header_meta_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_meta_ID='%s', header_meta_name='%s', header_meta_desc='%s')>" % \
        (str(self.file_ID), str(self.header_meta_ID), self.header_meta_name, self.header_meta_desc)





class Chroms(MetaBase):
    __tablename__  = 'chrom'
    #__table_args__ = {'sqlite_autoincrement': True}
    #chrom_ID       = Column(Integer, Sequence('chrom_id'), primary_key=True, autoincrement=True )

    CHROM_SIZE     =  256

    chrom_ID       = Column(Integer           , primary_key=True)
    chrom_name     = Column(String(CHROM_SIZE), index=True, unique=True )
    #coords         = relationship("Coords", order_by="Coords.coord_ID", primaryjoin="Chroms.chrom_ID==foreign(Coords.chrom_ID)")

    def __repr__(self):
        return "<Chroms(chrom_ID='%s', chrom_name='%s')>" % ( str(self.chrom_ID), self.chrom_name )

class Format_col(MetaBase):
    __tablename__  = 'format_col'
    #__table_args__ = {'sqlite_autoincrement': True}
    #format_ID      = Column(Integer, Sequence('format_col_id'), primary_key=True, autoincrement=True )

    FORMAT_COL_SIZE =   32

    format_ID       = Column(Integer                , primary_key=True)
    format_str      = Column(String(FORMAT_COL_SIZE), index=True, unique=True )

    def __repr__(self):
        return "<Format_col(format_ID='%s', format_str='%s')>" % ( str(self.format_ID), self.format_str )

class Refs(MetaBase):
    __tablename__  = 'refs'
    #__table_args__ = {'sqlite_autoincrement': True}
    #ref_ID         = Column(Integer, Sequence('refs_id'), primary_key=True, autoincrement=True)

    REF_SIZE       = 2048

    ref_ID         = Column(Integer         , primary_key=True)
    ref_str        = Column(String(REF_SIZE))

    def __repr__(self):
        return "<Refs(ref_ID='%s', ref_str='%s')>" % ( str(self.ref_ID), self.ref_str )

class Alts(MetaBase):
    __tablename__  = 'alts'
    #__table_args__ = {'sqlite_autoincrement': True}
    #alt_ID        = Column(Integer, Sequence('alts_id'), primary_key=True, autoincrement=True)

    ALT_SIZE      = 2048

    alt_ID        = Column(Integer         , primary_key=True)
    alt_str       = Column(String(ALT_SIZE)                  )

    def __repr__(self):
        return "<Alts(alts_ID='%s', alts_str='%s')>" % ( str(self.alt_ID), self.alt_str )

class VarType(MetaBase):
    __tablename__   = 'types'
    #__table_args__  = {'sqlite_autoincrement': True}
    #var_type_ID     = Column(Integer, Sequence('types_id'), primary_key=True, autoincrement=True)

    VAR_TYPE_SIZE   = 16

    var_type_ID     = Column(Integer              , primary_key=True)
    var_type_str    = Column(String(VAR_TYPE_SIZE), index=True, unique=True )

    def __repr__(self):
        return "<VarType(var_type_ID='%s', var_type_str='%s')>" % ( str(self.var_type_ID), self.var_type_str )

class VarSubType(MetaBase):
    __tablename__   = 'subtypes'
    #__table_args__  = {'sqlite_autoincrement': True}
    #var_subtype_ID  = Column(Integer, Sequence('subtypes_id'), primary_key=True, autoincrement=True)

    VAR_SUBTYPE_SIZE = 16

    var_subtype_ID   = Column(Integer                 , primary_key=True)
    var_subtype_str  = Column(String(VAR_SUBTYPE_SIZE), index=True, unique=True )

    def __repr__(self):
        return "<VarSubType(var_subtype_ID='%s', var_subtype_str='%s')>" % ( str(self.var_subtype_ID), self.var_subtype_str )




class Coords(DataBase):
    __tablename__         = 'coords'
    #__table_args__        = {'sqlite_autoincrement': True}

    CHROM_SIZE                    =  256
    FORMAT_COL_SIZE =   32

    COORDS_FILTER_SIZE            =   16
    COORDS_ID_SIZE                =   64
    COORDS_INFO_NAME_SIZE         =   16
    COORDS_INFO_VALUE_SIZE        =   16
    COORDS_META_NAME_SIZE         =   16
    COORDS_META_VALUE_SIZE        =   16
    COORDS_SAMPLE_NAME_SIZE       =  256
    COORDS_SAMPLE_INFO_NAME_SIZE  =   16
    COORDS_SAMPLE_INFO_VALUE_SIZE =   64
    COORDS_REF_SIZE               =  256
    COORDS_ALT_SIZE               =  256
    COORDS_FILTER_SIZE            =   32
    COORDS_META_ALLELE            =  256
    COORDS_META_SUBTYPE           =  256
    COORDS_META_TYPE              =  256
    COORDS_SAMPLE_GT              =   16
    COORDS_SAMPLE_GT_BASES        =  256
    FILEPATH_SIZE                 = 1024


    #file_ID               = Column(Integer                   , primary_key=True )
    file_path             = Column(String(FILEPATH_SIZE     ), primary_key=True )
    Chrom                 = Column(String(CHROM_SIZE        ), primary_key=True )
    Pos                   = Column(Integer                   , primary_key=True )
    Ref                   = Column(String(COORDS_REF_SIZE   ), index=True, nullable=False )
    Alt                   = Column(String(COORDS_ALT_SIZE   ), index=True, nullable=False )
    Format                = Column(String(FORMAT_COL_SIZE   ), index=True, nullable=False )
    #chrom_ID              = Column(Integer, index=True, nullable=False )

    #coord_ID              = Column(Integer                   , Sequence('coord_id'), primary_key=True, autoincrement=True )
    #coord_ID              = Column(Integer                   , primary_key=True, nullable=False )
    #file_ID               = Column(Integer                   , primary_key=True, index=True, nullable=False )
    #chrom_ID              = Column(Integer                   , index=True, nullable=False )
    #Pos                   = Column(Integer                   , primary_key=True, index=True, nullable=False )
    #format_ID             = Column(Integer                   , index=True, nullable=False )
    #ref_ID                = Column(Integer                   , index=True, nullable=False )
    #alt_ID                = Column(Integer                   , index=True, nullable=False )
    Qual                  = Column(Float                     , index=True, nullable=False )
    Filter                = Column(String(COORDS_FILTER_SIZE), index=True                 )
    Id                    = Column(String(COORDS_ID_SIZE    ), index=True                 )

    info_AF1              = Column(Float                     , index=True)
    info_CI95_1           = Column(Float                     , index=True)
    info_CI95_2           = Column(Float                     , index=True)
    info_DP               = Column(Integer                   , index=True)
    info_DP4_1            = Column(Integer                   , index=True)
    info_DP4_2            = Column(Integer                   , index=True)
    info_DP4_3            = Column(Integer                   , index=True)
    info_DP4_4            = Column(Integer                   , index=True)
    info_FQ               = Column(Integer                   , index=True)
    info_MQ               = Column(Integer                   , index=True)

    meta_aaf_1            = Column(Float                      , index=True)
    meta_aaf_2            = Column(Float                      , index=True)
    meta_aaf_3            = Column(Float                      , index=True)
    #meta_alleles_1        = Column(String(COORDS_META_ALLELE ), index=True)
    #meta_alleles_2        = Column(String(COORDS_META_ALLELE ), index=True)
    meta_call_rate        = Column(Float                      , index=True)
    meta_end              = Column(Integer                    , index=True)
    meta_heterozygosity   = Column(Float                      , index=True)
    meta_is_deletion      = Column(Boolean                    , index=True)
    meta_is_indel         = Column(Boolean                    , index=True)
    meta_is_monomorphic   = Column(Boolean                    , index=True)
    meta_is_snp           = Column(Boolean                    , index=True)
    meta_is_sv_precise    = Column(Boolean                    , index=True)
    meta_is_sv            = Column(Boolean                    , index=True)
    meta_is_transition    = Column(Boolean                    , index=True)
    meta_nucl_diversity   = Column(Float                      , index=True)
    meta_num_called       = Column(Integer                    , index=True)
    meta_num_het          = Column(Integer                    , index=True)
    meta_num_hom_alt      = Column(Integer                    , index=True)
    meta_num_hom_ref      = Column(Integer                    , index=True)
    meta_num_unknown      = Column(Integer                    , index=True)
    meta_start            = Column(Integer                    , index=True)
    meta_sv_end           = Column(Integer                    , index=True)
    #meta_var_type         = Column(Integer                   , index=True)
    #meta_var_subtype      = Column(Integer                   , index=True)
    meta_var_type         = Column(String(COORDS_META_TYPE   ), index=True)
    meta_var_subtype      = Column(String(COORDS_META_TYPE   ), index=True)

    sample_1_called       = Column(Boolean                       )
    sample_1_DP           = Column(Integer                       )
    sample_1_GQ           = Column(Integer                       )
    sample_1_GT           = Column(String(COORDS_SAMPLE_GT      ))
    sample_1_PL_1         = Column(Integer                       )
    sample_1_PL_2         = Column(Integer                       )
    sample_1_PL_3         = Column(Integer                       )
    sample_1_PL_4         = Column(Integer                       )
    sample_1_PL_5         = Column(Integer                       )
    sample_1_PL_6         = Column(Integer                       )
    sample_1_PL_7         = Column(Integer                       )
    sample_1_PL_8         = Column(Integer                       )
    sample_1_PL_9         = Column(Integer                       )
    sample_1_PL_10        = Column(Integer                       )
    sample_1_gt_alleles_1 = Column(Integer                       )
    sample_1_gt_alleles_2 = Column(Integer                       )
    #sample_1_gt_bases     = Column(String(COORDS_SAMPLE_GT_BASES))
    sample_1_gt_type      = Column(Integer                       )
    sample_1_is_het       = Column(Boolean                       )
    sample_1_is_variant   = Column(Boolean                       )
    sample_1_phased       = Column(Boolean                       )
    sample_1_name         = Column(Integer                       )




    #UniqueConstraint(file_ID, header_info_name)
    #ForeignKeyConstraint(['chrom_ID', 'file_ID'], ['chrom.chrom_ID', 'files.file_ID'])

    #-COORDS
    #--coord_ID[AUTOINC]$
    #--file_ID[EXTKEY]$!
    #--format_ID[EXTKEY]$!
    #--chrom_ID[EXTKEY]$!
    #--ref_ID[EXTKEY]$!
    #--alt_ID[EXTKEY]$!
    #--filter[str]
    #--id[STR]*
    #--pos[INT]*
    #--qual[FLOAT]*

    def __repr__(self):
        return "<File='%s', Chrom='%s', Pos='%d', Ref='%s', Alt='%s', Filter='%s', Id='%s', Qual='%.3f')>" % \
        (self.file_path, self.Chrom, self.Pos, str(self.Ref), str(self.Alt), \
         str(self.Filter) , str(self.Id)      , self.Qual)

#Index('__coord_chrom_pos'     , Coords.chrom_ID, Coords.Pos)
#Index('__coord_chrom_pos_qual', Coords.chrom_ID, Coords.Pos, Coords.Qual)
Index('__coord_chrom_pos'     , Coords.Chrom, Coords.Pos)
Index('__coord_chrom_pos_qual', Coords.Chrom, Coords.Pos, Coords.Qual)



dbs = (Chroms, Format_col, Refs, Alts, Files, Header, Header_format, Header_info, Header_meta, Coords)

if __name__ == '__main__':
    main(sys.argv[1:])


#CREATE TABLE refs (
#        "ref_ID" INTEGER NOT NULL,
#        ref_str VARCHAR NOT NULL,
#        PRIMARY KEY ("ref_ID", ref_str),
#        UNIQUE (ref_str)
#)
#
#CREATE TABLE coords_sample_info (
#        "coord_sample_info_ID" INTEGER NOT NULL,
#        coord_sample_info_name VARCHAR NOT NULL,
#        "coord_sample_info_valueS" VARCHAR,
#        "coord_sample_info_valueI" INTEGER,
#        "coord_sample_info_valueF" FLOAT,
#        "coord_sample_info_valueB" BOOLEAN,
#        PRIMARY KEY ("coord_sample_info_ID", coord_sample_info_name),
#        CHECK ("coord_sample_info_valueB" IN (0, 1))
#)
#
#CREATE TABLE chrom (
#        "chrom_ID" INTEGER NOT NULL,
#        chrom_name VARCHAR NOT NULL,
#        PRIMARY KEY ("chrom_ID", chrom_name),
#        UNIQUE (chrom_name)
#)
#
#CREATE TABLE header (
#        "header_ID" INTEGER NOT NULL,
#        header_name VARCHAR NOT NULL,
#        header_value VARCHAR,
#        PRIMARY KEY ("header_ID", header_name),
#        UNIQUE (header_name)
#)
#
#CREATE TABLE alts (
#        "alts_ID" INTEGER NOT NULL,
#        alts_str VARCHAR NOT NULL,
#        PRIMARY KEY ("alts_ID", alts_str),
#        UNIQUE (alts_str)
#)
#
#CREATE TABLE header_info (
#        "header_info_ID" INTEGER NOT NULL,
#        header_info_name VARCHAR NOT NULL,
#        header_info_type VARCHAR,
#        header_info_desc VARCHAR,
#        header_info_args INTEGER,
#        PRIMARY KEY ("header_info_ID", header_info_name),
#        UNIQUE (header_info_name)
#)
#
#CREATE TABLE coords_meta (
#        "coord_meta_ID" INTEGER NOT NULL,
#        coord_meta_name VARCHAR NOT NULL,
#        "coord_meta_valueS" VARCHAR,
#        "coord_meta_valueI" INTEGER,
#        "coord_meta_valueF" FLOAT,
#        "coord_meta_valueB" BOOLEAN,
#        PRIMARY KEY ("coord_meta_ID", coord_meta_name),
#        CHECK ("coord_meta_valueB" IN (0, 1))
#)
#
#CREATE TABLE header_format (
#        "header_format_ID" INTEGER NOT NULL,
#        header_format_name VARCHAR NOT NULL,
#        header_format_type VARCHAR,
#        header_format_desc VARCHAR,
#        header_format_args INTEGER,
#        PRIMARY KEY ("header_format_ID", header_format_name),
#        UNIQUE (header_format_name)
#)
#
#CREATE TABLE coords_info (
#        "coord_info_ID" INTEGER NOT NULL,
#        coord_info_name VARCHAR NOT NULL,
#        "coord_info_valueS" VARCHAR,
#        "coord_info_valueI" INTEGER,
#        "coord_info_valueF" FLOAT,
#        "coord_info_valueB" BOOLEAN,
#        PRIMARY KEY ("coord_info_ID", coord_info_name),
#        CHECK ("coord_info_valueB" IN (0, 1))
#)
#
#CREATE TABLE coords_sample (
#        "coord_sample_ID" INTEGER NOT NULL,
#        coord_sample_name VARCHAR NOT NULL,
#        PRIMARY KEY ("coord_sample_ID", coord_sample_name)
#)
#
#CREATE TABLE files (
#        "file_ID" INTEGER NOT NULL,
#        file_path VARCHAR NOT NULL,
#        file_base VARCHAR,
#        file_name VARCHAR,
#        PRIMARY KEY ("file_ID", file_path),
#        UNIQUE (file_path)
#)
#
#CREATE TABLE format_col (
#        "format_ID" INTEGER NOT NULL,
#        format_str VARCHAR NOT NULL,
#        PRIMARY KEY ("format_ID", format_str),
#        UNIQUE (format_str)
#)
#
#CREATE TABLE header_meta (
#        "header_meta_ID" INTEGER NOT NULL,
#        header_meta_name VARCHAR NOT NULL,
#        header_meta_desc VARCHAR,
#        PRIMARY KEY ("header_meta_ID", header_meta_name),
#        UNIQUE (header_meta_name)
#)
#
#CREATE TABLE coords (
#        "coord_ID" INTEGER NOT NULL,
#        "Pos" INTEGER NOT NULL,
#        "Filter" VARCHAR,
#        "Id" VARCHAR,
#        "Qual" FLOAT,
#        "format_ID" INTEGER,
#        "chrom_ID" INTEGER,
#        "ref_ID" INTEGER,
#        "alt_ID" INTEGER,
#        PRIMARY KEY ("coord_ID", "Pos"),
#        FOREIGN KEY("format_ID") REFERENCES format_col ("format_ID"),
#        FOREIGN KEY("chrom_ID") REFERENCES chrom ("chrom_ID"),
#        FOREIGN KEY("ref_ID") REFERENCES refs ("ref_ID"),
#        FOREIGN KEY("alt_ID") REFERENCES alts ("alts_ID")
#)

"""
fileReg   = Files(file_path=infile_abs_path, file_base=infile_basename, file_name=infile)
coordsReg = fileReg.coords
coordReg  = Coords(Pos=rec['POS'], Filter=rec['FILTER'], Id=rec['ID'], Qual=rec['QUAL'])
coordsReg.append( coordReg )
coordReg

, format_ID=rec['FORMAT'], chrom_ID=rec['CHROM'], ref_ID=rec['REF'], alt_ID=rec['ALT']

session.add( coord )
session.commit()
"""

#$ - primary key
#* - indexed
#! - external key
#
#DB:
#-FILES:
#--file_ID[AUTOINC]
#--file_base[STR]
#--file_name[STR]
#--file_path[STR]$
#-HEADER
#--file_ID[EXTKEY]$!
#--header_name[STR]*
#--header_value[STR]
#-FORMATS:
#--file_ID[EXTKEY]$!
#--key_name[STR]$
#--key_type[STR]
#--key_desc[STR]
#--key_args[INT]
#-INFOS:
#--file_ID[EXTKEY]$!
#--info_name[STR]$
#--info_type[STR]
#--info_desc[STR]
#--info_args[INT]
#-META
#--file_ID[EXTKEY]$!
#--meta_name[STR]
#--meta_desc[STR]
#-COORDS
#--coord_ID[AUTOINC]$
#--file_ID[EXTKEY]$!
#--format_ID[EXTKEY]$!
#--chrom_ID[EXTKEY]$!
#--ref_ID[EXTKEY]$!
#--alt_ID[EXTKEY]$!
#--filter[str]
#--id[STR]*
#--pos[INT]*
#--qual[FLOAT]*
#-COORDS_INFO
#--coord_ID[EXTKEY]$!
#--coord_info_name[STR]$
#--coord_info_value[STR]*
#-COORDS_NFO
#--coord_ID[EXTKEY]$!
#--coord_info_ID[AUTOINC]$
#--coord_info_name[STR]$
#--coord_info_value[STR]
#-COODS_SAMPLES
#--coord_ID[EXTKEY]$!
#--coord_sample_ID[AUTOINC]$
#--coord_sample_name[STR]*
#--coord_sample_info_name[STR]*
#--coord_sample_info_value[STR]*
#-CHROMS
#--chrom_ID[AUTOINC]$
#--chrom_name[STR]$
#-FORMATS
#--format_ID[AUTOINC]$
#--format_str[STR]$
#-REFS
#--ref_ID[AUTOINC]$
#--ref_nuc[STR]
#-ALTS
#--alt_ID[AUTOINC]$
#--alt_nuc[STR]$
