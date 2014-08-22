#!/usr/bin/python
import os
import sys
import inspect

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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship, backref

print sqlalchemy.__version__


def main(args, echo=True):
    #dbname   = 'test.sqlite'
    #dbname   = 'sqlite:///:memory:'
    dbname   = args[0]
    if os.path.exists( dbname ):
        os.remove( dbname )
    loaddb(dbname, echo=echo)

class loaddb(object):
    def __init__(self, dbname, echo=False):
        print "creating engine", dbname

        self.cache   = {}

        self.dbname  = 'sqlite:///' + dbname

        self.engine  = create_engine(self.dbname, echo=echo)

        if self.dbname == 'sqlite:///:memory:' or not os.path.exists(self.dbname):
            print "creating database"
            Base.metadata.create_all(self.engine)

        print "creating session"
        self.Session = sessionmaker(bind=self.engine, autoflush=False, expire_on_commit=False)
        self.session = self.Session()

        self.meta    = MetaData()
        self.meta.reflect(bind=self.engine)
        self.insp    = reflection.Inspector.from_engine( self.engine )

        print "finished"

    def get_session(self):
        return self.session

    def get_or_update( self, cls, att, val ):

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

        for name in self.insp.get_table_names():
            if table_name is not None and table_name != name:
                continue

            if name not in indexes:
                indexes[ name ] = []

            for index in self.insp.get_indexes(name):
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
        for tbl in indexes:
            if table_name is not None and table_name != tbl:
                continue

            for nfo in indexes[ tbl ]:
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



Base = declarative_base()


CHROM_SIZE                    =  256
FORMAT_COL_SIZE               =   32
#REF_SIZE                      = 2048
#ALT_SIZE                      = 2048
FILEPATH_SIZE                 = 1024
FILEBASE_SIZE                 =  512
FILENAME_SIZE                 =  256

HEADER_NAME_SIZE              =   32
HEADER_VALUE_SIZE             =  512
HEADER_FORMAT_NAME_SIZE       =   32
HEADER_FORMAT_TYPE_SIZE       =   16
HEADER_FORMAT_DESC_SIZE       =  256
HEADER_INFO_NAME_SIZE         =   16
HEADER_INFO_TYPE_SIZE         =   16
HEADER_INFO_DESC_SIZE         =   64
HEADER_META_NAME_SIZE         =   32
HEADER_META_DESC_SIZE         =  512

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




class Chroms(Base):
    __tablename__  = 'chrom'
    __table_args__ = {'sqlite_autoincrement': True}
    chrom_ID       = Column(Integer, Sequence('chrom_id'), primary_key=True, autoincrement=True )
    chrom_name     = Column(String , index=True, unique=True )
    #coords         = relationship("Coords", order_by="Coords.coord_ID", primaryjoin="Chroms.chrom_ID==foreign(Coords.chrom_ID)")

    def __repr__(self):
        return "<Chroms(chrom_ID='%s', chrom_name='%s')>" % ( str(self.chrom_ID), self.chrom_name )

class Format_col(Base):
    __tablename__  = 'format_col'
    __table_args__ = {'sqlite_autoincrement': True}
    format_ID      = Column(Integer, Sequence('format_col_id'), primary_key=True, autoincrement=True )
    format_str     = Column(String , index=True, unique=True )

    def __repr__(self):
        return "<Format_col(format_ID='%s', format_str='%s')>" % ( str(self.format_ID), self.format_str )

class Refs(Base):
    __tablename__  = 'refs'
    __table_args__ = {'sqlite_autoincrement': True}
    ref_ID         = Column(Integer, Sequence('refs_id'), primary_key=True, autoincrement=True)
    ref_str        = Column(String , index=True, unique=True )

    def __repr__(self):
        return "<Refs(ref_ID='%s', ref_str='%s')>" % ( str(self.ref_ID), self.ref_str )

class Alts(Base):
    __tablename__  = 'alts'
    __table_args__ = {'sqlite_autoincrement': True}
    alt_ID        = Column(Integer, Sequence('alts_id'), primary_key=True, autoincrement=True)
    alt_str       = Column(String , index=True, unique=True )

    def __repr__(self):
        return "<Alts(alts_ID='%s', alts_str='%s')>" % ( str(self.alt_ID), self.alt_str )

class VarType(Base):
    __tablename__   = 'types'
    __table_args__  = {'sqlite_autoincrement': True}
    var_type_ID     = Column(Integer, Sequence('types_id'), primary_key=True, autoincrement=True)
    var_type_str    = Column(String , index=True, unique=True )

    def __repr__(self):
        return "<VarType(var_type_ID='%s', var_type_str='%s')>" % ( str(self.var_type_ID), self.var_type_str )

class VarSubType(Base):
    __tablename__   = 'subtypes'
    __table_args__  = {'sqlite_autoincrement': True}
    var_subtype_ID  = Column(Integer, Sequence('subtypes_id'), primary_key=True, autoincrement=True)
    var_subtype_str = Column(String , index=True, unique=True )

    def __repr__(self):
        return "<VarSubType(var_subtype_ID='%s', var_subtype_str='%s')>" % ( str(self.var_subtype_ID), self.var_subtype_str )





class Files(Base):
    __tablename__  = 'files'
    __table_args__ = {'sqlite_autoincrement': True}
    file_ID        = Column(Integer, Sequence('files_id'), primary_key=True, autoincrement=True )
    file_path      = Column(String , unique=True         , index=True                           )
    file_base      = Column(String)
    file_name      = Column(String)

    #-FILES:
    #--file_ID[AUTOINC]
    #--file_base[STR]
    #--file_name[STR]
    #--file_path[STR]$

    def __repr__(self):
        return "<Files(file_ID='%s', file_path='%s', file_base='%s', file_name='%s')>" % \
        ( str(self.file_ID), self.file_path, self.file_base, self.file_name )

class Header(Base):
    __tablename__  = 'header'
    __table_args__ = {'sqlite_autoincrement': True}
    header_ID      = Column(Integer, Sequence('header_id'), primary_key=True, autoincrement=True )
    header_name    = Column(String , index=True )
    header_value   = Column(String)
    file_ID        = Column(Integer, ForeignKey('files.file_ID'))
    file_src       = relationship("Files", backref=backref('headers', order_by=header_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header(file_ID='%s', header_ID='%s', header_name='%s', header_value='%s')>" % \
                (str( self.file_ID ), str( self.header_ID ),
                 self.header_name, self.header_value)

class Header_format(Base):
    __tablename__      = 'header_format'
    __table_args__     = {'sqlite_autoincrement': True}
    header_format_ID   = Column(Integer, Sequence('header_format_id'), primary_key=True, autoincrement=True )
    header_format_name = Column(String , index=True )
    header_format_num  = Column(Integer)
    header_format_type = Column(String )
    header_format_desc = Column(String )
    file_ID            = Column(Integer, ForeignKey('files.file_ID'))
    file_src           = relationship("Files", backref=backref('header_formats', order_by=header_format_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_format_ID='%s', header_format_name='%s', header_format_num='%d', header_format_type='%s', header_format_desc='%s')>" % \
        (str(self.file_ID), str(self.header_format_ID),
         self.header_format_name, self.header_format_num, self.header_format_type, self.header_format_desc)

class Header_info(Base):
    __tablename__    = 'header_info'
    __table_args__   = {'sqlite_autoincrement': True}
    header_info_ID   = Column(Integer, Sequence('header_info_id'), primary_key=True, autoincrement=True )
    header_info_name = Column(String , index=True )
    header_info_num  = Column(Integer)
    header_info_type = Column(String )
    header_info_desc = Column(String )
    file_ID          = Column(Integer, ForeignKey('files.file_ID'))
    file_src         = relationship("Files", backref=backref('header_infos', order_by=header_info_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_info_ID='%s', header_info_name='%s', header_info_num='%d', header_info_type='%s', header_info_desc='%s')>" % \
        (str(self.file_ID), str(self.header_info_ID), self.header_info_name, self.header_info_num, self.header_info_type, self.header_info_desc)

class Header_meta(Base):
    __tablename__    = 'header_meta'
    __table_args__   = {'sqlite_autoincrement': True}
    header_meta_ID   = Column(Integer, Sequence('header_meta_id'), primary_key=True, autoincrement=True )
    header_meta_name = Column(String , index=True )
    header_meta_desc = Column(String )
    file_ID          = Column(Integer, ForeignKey('files.file_ID'))
    file_src         = relationship("Files", backref=backref('header_metas', order_by=header_meta_ID), cascade="all, delete")

    def __repr__(self):
        return "<Header_meta(file_ID='%s', header_meta_ID='%s', header_meta_name='%s', header_meta_desc='%s')>" % \
        (str(self.file_ID), str(self.header_meta_ID), self.header_meta_name, self.header_meta_desc)

class Coords(Base):
    __tablename__         = 'coords'
    __table_args__        = {'sqlite_autoincrement': True}

    #Chrom                 = Column(String(CHROM_SIZE        ), index=True, nullable=False )
    #Format                = Column(String(FORMAT_COL_SIZE   ), index=True, nullable=False )
    #Ref                   = Column(String(COORDS_REF_SIZE   ), index=True, nullable=False )
    #Alt                   = Column(String(COORDS_ALT_SIZE   ), index=True, nullable=False )
    #chrom_ID              = Column(Integer, index=True, nullable=False )

    coord_ID              = Column(Integer, Sequence('coord_id'), primary_key=True, autoincrement=True )
    file_ID               = Column(Integer                   , index=True, nullable=False )
    chrom_ID              = Column(Integer                   , index=True, nullable=False )
    Pos                   = Column(Integer                   , index=True, nullable=False )
    format_ID             = Column(Integer                   , index=True, nullable=False )
    ref_ID                = Column(Integer                   , index=True, nullable=False )
    alt_ID                = Column(Integer                   , index=True, nullable=False )
    Qual                  = Column(Float                     , index=True, nullable=False )
    Filter                = Column(String(COORDS_FILTER_SIZE), index=True                 )
    Id                    = Column(String(COORDS_ID_SIZE    ), index=True                 )

    info_AF1              = Column(Float  , index=True)
    info_CI95_1           = Column(Float  , index=True)
    info_CI95_2           = Column(Float  , index=True)
    info_DP               = Column(Integer, index=True)
    info_DP4_1            = Column(Integer, index=True)
    info_DP4_2            = Column(Integer, index=True)
    info_DP4_3            = Column(Integer, index=True)
    info_DP4_4            = Column(Integer, index=True)
    info_FQ               = Column(Integer, index=True)
    info_MQ               = Column(Integer, index=True)
    # 'INFO': {'AF1': 1.0,
    #          'CI95': [1.0, 1.0],
    #          'DP': 38,
    #          'DP4': [0, 0, 19, 19],
    #          'FQ': -141.0,
    #          'MQ': 60},
    meta_aaf_1            = Column(Float                      , index=True)
    meta_aaf_2            = Column(Float                      , index=True)
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
    #meta_var_subtype      = Column(String(COORDS_META_SUBTYPE), index=True)
    #meta_var_type         = Column(String(COORDS_META_TYPE   ), index=True)
    meta_var_type         = Column(String(COORDS_META_TYPE   ), index=True)

    # 'nfo': {'aaf': [1.0],
    #         'alleles': ['A', 'G'],
    #         'call_rate': 1.0,
    #         'end': 3235,
    #         'heterozygosity': 0.0,
    #         'is_deletion': False,
    #         'is_indel': False,
    #         'is_monomorphic': False,
    #         'is_snp': True,
    #         'is_sv': False,
    #         'is_sv_precise': False,
    #         'is_transition': True,
    #         'nucl_diversity': 0.0,
    #         'num_called': 1,
    #         'num_het': 0,
    #         'num_hom_alt': 1,
    #         'num_hom_ref': 0,
    #         'num_unknown': 0,
    #         'start': 3234,
    #         'sv_end': None,
    #         'var_subtype': 'ts',
    #         'var_type': 'snp'},
    sample_1_called       = Column(Boolean                       )
    sample_1_DP           = Column(Integer                       )
    sample_1_GQ           = Column(Integer                       )
    sample_1_GT           = Column(String(COORDS_SAMPLE_GT      ))
    sample_1_PL_1         = Column(Integer                       )
    sample_1_PL_2         = Column(Integer                       )
    sample_1_PL_3         = Column(Integer                       )
    sample_1_gt_alleles_1 = Column(Integer                       )
    sample_1_gt_alleles_2 = Column(Integer                       )
    #sample_1_gt_bases     = Column(String(COORDS_SAMPLE_GT_BASES))
    sample_1_gt_type      = Column(Integer                       )
    sample_1_is_het       = Column(Boolean                       )
    sample_1_is_variant   = Column(Boolean                       )
    sample_1_phased       = Column(Boolean                       )
    sample_1_name         = Column(Integer                       )
    #         'samples': [{'called': True,
    #                      'data': {'DP': 38,
    #                               'GQ': 99,
    #                               'GT': '1/1',
    #                               'PL': [255, 114, 0]},
    #                      'gt_alleles': ['1', '1'],
    #                      'gt_bases': 'G/G',
    #                      'gt_type': 2,
    #                      'is_het': False,
    #                      'is_variant': True,
    #                      'phased': False,
    #                      'sample': '/panfs/ANIMAL/group001/minjiumeng/tomato_reseq/SZAXPI008746-45'}],


    #         'get_hom_refs': [],
    #         'get_unknowns': [],
    #         'get_hets': [],
    #         'get_hom_alts': [{'called': True,
    #                           'data': {'DP': 38,
    #                                    'GQ': 99,
    #                                    'GT': '1/1',
    #                                    'PL': [255, 114, 0]},
    #                           'gt_alleles': ['1', '1'],
    #                           'gt_bases': 'G/G',
    #                           'gt_type': 2,
    #                           'is_het': False,
    #                           'is_variant': True,
    #                           'phased': False,
    #                           'sample': '/panfs/ANIMAL/group001/minjiumeng/tomato_reseq/SZAXPI008746-45'}],

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
        return "<Coords(file_ID='%s', coord_ID='%s', format_ID='%s', chrom_ID='%s', ref_ID='%s', alt_ID='%s', Filter='%s', Id='%s', Pos='%d', Qual='%.3f')>" % \
        (str(self.file_ID), str(self.coord_ID), str(self.format_ID), str(self.chrom_ID), str(self.ref_ID), str(self.alt_ID), \
         str(self.Filter) , str(self.Id)      , self.Pos           , self.Qual)


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
