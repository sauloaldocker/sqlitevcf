#!/usr/bin/python
#http://api.mongodb.org/python/current/tutorial.html
import sys
import os
import pprint
from itertools import izip
#import json
#import urllib
import argparse
import time
from collections import OrderedDict
#from pymongo       import MongoClient
#from pymongo       import ASCENDING, DESCENDING
#from pymongo       import errors
#from bson.objectid import ObjectId
#from bson          import json_util

import vcf

import datetime

sys.path.insert(0, '.')
from database import *

ppp = pprint.PrettyPrinter(indent=1)
pp  = ppp.pprint

sql_echo        = False

#MAX UPDATE: 5 * 430 = 2150 RECS/S 10 * 230 = 2300 RECS/S
#MAX INSERT: 5 * 500 = 2500 RECS/S 10 * 300 = 3000 RECS/S
#./mongo_vcf.py db_del -s localhost -p 27017 -d vcf -y; ./mongo_vcf.py db_add -s localhost -p 27017 -d vcf ; ./mongo_vcf.py file_add -s localhost -p 27017 -d vcf RF_001_SZAXPI008746-45.vcf.gz.snpeff.vcf
#./mongo_vcf.py db_del -s localhost -p 27017 -d vcf -y; ./mongo_vcf.py db_add -s localhost -p 27017 -d vcf ; find data/ -name '*.vcf' | sort | xargs -P2 -n1 ./mongo_vcf.py file_add -s localhost -p 27017 -d vcf



def main(args):
    print args
    indb    = args[0 ]
    infiles = args[1:]
    db      = loaddb(indb, echo=sql_echo)

    print "droping indexes"
    indexes        = db.list_indexes( table_name='coords' )
    db.drop_indexes(table_name='coords')

    print infiles
    for infile in infiles:
        process_file( db, infile )

    print "adding indexes"
    db.add_indexes(indexes, 'coords')
    print "finished"


def diffTime( runid, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap ):
    currTime       = time.time()

    diffTimeStart  = currTime - startTime
    diffTimeChrom  = currTime - startTimeChrom
    diffTimeLap    = currTime - startTimeLap

    diffRegsStart  = float( regs      )
    diffRegsChrom  = float( regsChrom )
    diffRegsLap    = float( regsLap   )

    speedStart     = diffRegsStart  / diffTimeStart
    speedChrom     = diffRegsChrom  / diffTimeChrom
    speedLap       = diffRegsLap    / diffTimeLap

    print "\
%s :: Time   : Start %7ds      Chrom %s %7ds      Lap %7ds\n\
%s :: Records: Start %7d       Chrom %s %7d       Lap %7d\n\
%s :: Speed  : Start %7.1f rec/s Chrom %s %7.1f rec/s Lap %7.1f rec/s\n\n" % (  runid, diffTimeStart, chrom, diffTimeChrom, diffTimeLap,
                                                                                runid, diffRegsStart, chrom, diffRegsChrom, diffRegsLap,
                                                                                runid, speedStart   , chrom, speedChrom   , speedLap )
    #sys.stdout.flush()


def add_header(session, vcf_reader, fileReg):
    headers = {
                'header'  : [],
                'formats' : [],
                'infos'   : [],
                'metadata': [],
           }


    for header_key in dir(vcf_reader):
        if header_key[0] == '_':
            continue

        header_val = getattr(vcf_reader, header_key, None)

        if header_val is None:
            continue

        if str(type(header_val)) == "<type 'generator'>":
            continue

        if str(type(header_val)) == "<type 'instancemethod'>":
            continue

        #print "HEADER KEY ", header_key
        #print "HEADER VAL ", header_val
        #print "HEADER TYPE", type(header_val)



        header_type = None
        if   type(header_val) == type(OrderedDict()):
            #print " ORDERED DICT"
            header_type = 'dict'

        elif type(header_val) == type(list()):
            #print " LIST"
            header_type = 'list'

        elif type(header_val) == type(str()):
            #print " STRING"
            header_type = 'string'



        if header_type == 'string':
            #print "  ADDING STRING"
            header = None

            if   header_key == 'formats':
                pass

            elif header_key == 'infos':
                pass

            elif header_key == 'metadata':
                pass

            else:
                header = Header(header_name=header_key, header_value=header_val)
                headers['header'].append( header )
                #print "    APPENDED"



        elif header_type == 'list':
            #print "  ADDING LIST"
            for el in header_val:
                #print "  ADDING LIST EL", el
                header = Header(header_name=header_key, header_value=el)
                headers['header'].append( header )
                #print "    APPENDED"



        elif header_type == 'dict':
            #print "  ADDING DICT"
            for el_name in header_val:
                el_val = header_val[el_name]
                #print "  ADDING DICT EL_NAME", el_name
                #print "  ADDING DICT EL_VAL ", el_val

                if   header_key == 'formats':
                    header_format = Header_format(header_format_name=el_name, header_format_num=el_val[1], header_format_type=el_val[2], header_format_desc=el_val[3])
                    headers['formats'].append( header_format )
                    #print "    APPENDED"

                elif header_key == 'infos':
                    ## fix snpeff
                    #if 'EFF' in infos:
                    #    #print infos
                    #
                    #    for k in infos.keys():
                    #        #print k, infos[k]
                    #        #print 'DICT', infos[k].__dict__
                    #        #print 'ASDICT', infos[k]._asdict()
                    #        #, dir(infos[k])
                    #        infos[k] = dict( infos[k]._asdict() )
                    #
                    #    #print 'INFOS', infos
                    #    #print 'INFOS EFF', infos['EFF']
                    #    #print 'INFOS EFF DESC', infos['EFF']['desc']
                    #
                    #    #infos['EFF']['desc'] = [ x.replace('[', '').replace(']', '').replace(')', '').replace('(', '|').strip().split( '|' ) for x in infos['EFF']['desc'] ]
                    #    Spos = infos['EFF']['desc'].find('Format: ')
                    #    if Spos != -1:
                    #        Spos += 8
                    #        infos['EFF']['format'] = infos['EFF']['desc'][Spos:]
                    #        infos['EFF']['format'] = infos['EFF']['format'].replace('[', '').replace(']', '').replace(')', '').replace('\'', '').replace('(', '|').split( '|' )
                    #        infos['EFF']['format'] = [ x.strip() for x in infos['EFF']['format'] ]
                    #        #print 'INFOS EFF FORMAT', infos['EFF']['format']


                    header_info = Header_info(header_info_name=el_name, header_info_num=el_val[1], header_info_type=el_val[2], header_info_desc=el_val[3])
                    headers['infos'].append( header_info )
                    #print "    APPENDED"

                elif header_key == 'metadata':
                    if   type(el_val) == type(list()):
                        for el_val_val in el_val:
                            header_meta = Header_meta(header_meta_name=el_name, header_meta_desc=el_val_val)
                            headers['metadata'].append( header_meta )
                            #print "    APPENDED"

                    elif type(el_val) == type(str()):
                        header_meta = Header_meta(header_meta_name=el_name, header_meta_desc=el_val)
                        headers['metadata'].append( header_meta )
                        #print "    APPENDED"

                else:
                    header = Header(header_name=header_key + ':' + el_name, header_value=el_val)
                    headers['header'].append( header )
                    #print "    APPENDED"



    #print 'HEADERS', headers

    for headern in headers:
        if   headern == 'formats':
            fileReg.header_formats = headers[headern]

        elif headern == 'infos':
            fileReg.header_infos   = headers[headern]

        elif headern == 'metadata':
            fileReg.header_metas   = headers[headern]

        elif headern == 'header':
            fileReg.headers        = headers[headern]

    session.add( fileReg )
    session.commit()
    session.flush()


def process_file( db, infile ):
    print "processing", infile

    colnames        = ('CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT' )

    #printevery      =  6400
    #dumpevery       =  6400
    #debug           = 12800 # -1 no; > 1 = delete database, number of samples to read

    #printevery      =  5
    #dumpevery       =  5
    #debug           = 10 # -1 no; > 1 = delete database, number of samples to read

    printevery      =  6400
    dumpevery       =  6400
    debug           =    -1 # -1 no; > 1 = delete database, number of samples to read

    infile_abs_path = os.path.abspath(  infile )
    infile_basename = os.path.basename( infile )

    vcf_reader      = vcf.Reader(open(infile, 'r'))

    session         = db.get_session()

    fileReg         = Files(file_path=infile_abs_path, file_base=infile_basename, file_name=infile)
    session.add( fileReg )
    session.commit()
    session.flush()

    add_header( session, vcf_reader, fileReg )

    samples_names   = getattr(vcf_reader, 'samples', [])

    file_ID         = fileReg.file_ID

    lastChrom       = None
    lastChromUrl    = None
    lastPos         = None
    regs            = 0
    regsChrom       = 0
    regsLap         = 0
    startTime       = time.time()
    startTimeChrom  = startTime
    startTimeLap    = startTime
    records         = [None] * dumpevery
    chrom_ID        = -1
    formats         = {}
    refs            = {}
    alts            = {}
    types           = {}
    subtypes        = {}
    get_or_update   = db.get_or_update
    execute         = db.engine.execute

    for record in vcf_reader:
        #print pp.pprint( record )

        regs      += 1
        regsChrom += 1
        regsLap   += 1

        rec = {}
        for k in colnames:
            val = getattr( record, k )
            #print k, val
            rec[ k ] = parseval( val )

        chrom = rec[ 'CHROM' ]
        pos   = rec[ 'POS'   ]



        #fix SNPeff
        if 'INFO' in rec and 'EFF' in rec['INFO']:
            #print "INFO EFF B", rec['INFO']['EFF']
            rec['INFO']['EFF'] = [ x.replace(')', '').replace('(', '|').strip().split( '|' ) for x in rec['INFO']['EFF'] ]
            #print "INFO EFF A", rec['INFO']['EFF']


        if lastChrom != chrom:
            #diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
            startTimeLap   = time.time()
            startTimeChrom = startTimeLap

            lastChrom      = chrom
            regsChrom      = 0
            regsLap        = 0

            db_chrom       = get_or_update( Chroms    , 'chrom_name', chrom         )
            chrom_ID       = db_chrom.chrom_ID

            print "\n", infile, chrom, chrom_ID, db_chrom

        #pp(rec)

        format_ID = formats.get(rec['FORMAT'], -1)
        if format_ID == -1:
            db_fmt    = get_or_update( Format_col, 'format_str', rec['FORMAT'] )
            format_ID = db_fmt.format_ID
            formats[ rec['FORMAT'] ] = format_ID


        ref_ID = refs.get(rec['REF'], -1)
        if ref_ID == -1:
            db_ref    = get_or_update( Refs      , 'ref_str'   , rec['REF'   ] )
            ref_ID    = db_ref.ref_ID
            refs[ rec['REF'] ] = ref_ID

        var_type_ID = types.get( record.var_type, -1 )
        if var_type_ID == -1:
            db_var_type = get_or_update( VarType      , 'var_type_str'   , record.var_type )
            var_type_ID = db_var_type.var_type_ID
            types[ record.var_type ] = var_type_ID

        var_subtype_ID = types.get( record.var_subtype, -1 )
        if var_subtype_ID == -1:
            db_var_subtype = get_or_update( VarSubType      , 'var_subtype_str'   , record.var_subtype )
            var_subtype_ID = db_var_subtype.var_subtype_ID
            types[ record.var_subtype ] = var_subtype_ID


        alt_str   = ','.join(rec['ALT'   ])
        alt_ID = alts.get(alt_str, -1)
        if alt_ID == -1:
            db_alt    = get_or_update( Alts      , 'alt_str'   , alt_str )
            alt_ID    = db_alt.alt_ID
            refs[ alt_str ] = alt_ID

        rec_samples   = sample2dict( record.samples )
	rec_samples_0 = rec_samples[0]
        if len( rec_samples ) != 1:
            print "more than one sample"
            sys.exit(1)

        rec_info     = rec['INFO']

        data = {
            'file_ID'               : file_ID,
            'chrom_ID'              : chrom_ID,
            'format_ID'             : format_ID,
            'ref_ID'                : ref_ID,
            'alt_ID'                : alt_ID,
            'Pos'                   : rec['POS'   ],
            'Qual'                  : rec['QUAL'  ],
            'Filter'                : rec['FILTER'],
            'Id'                    : rec['ID'    ],
            'info_AF1'              : rec_info['AF1' ],
            'info_CI95_1'           : rec_info['CI95'][0] if 'CI95' in rec_info else None,
            'info_CI95_2'           : rec_info['CI95'][1] if 'CI95' in rec_info else None,
            'info_DP'               : rec_info['DP'  ],
            'info_DP4_1'            : rec_info['DP4' ][0],
            'info_DP4_2'            : rec_info['DP4' ][1],
            'info_DP4_3'            : rec_info['DP4' ][2],
            'info_DP4_4'            : rec_info['DP4' ][3],
            'info_FQ'               : rec_info['FQ'  ],
            'info_MQ'               : rec_info['MQ'  ],
            'meta_aaf_1'            : record.aaf[0],
            'meta_aaf_2'            : record.aaf[1] if len(record.aaf) == 2 else None,
            'meta_call_rate'        : record.call_rate,
            'meta_end'              : record.end,
            'meta_heterozygosity'   : record.heterozygosity,
            'meta_is_deletion'      : record.is_deletion,
            'meta_is_indel'         : record.is_indel,
            'meta_is_monomorphic'   : record.is_monomorphic,
            'meta_is_snp'           : record.is_snp,
            'meta_is_sv'            : record.is_sv,
            'meta_is_sv_precise'    : record.is_sv_precise,
            'meta_is_transition'    : record.is_transition,
            'meta_nucl_diversity'   : record.nucl_diversity,
            'meta_num_called'       : record.num_called,
            'meta_num_het'          : record.num_het,
            'meta_num_hom_alt'      : record.num_hom_alt,
            'meta_num_hom_ref'      : record.num_hom_ref,
            'meta_num_unknown'      : record.num_unknown,
            'meta_start'            : record.start,
            'meta_sv_end'           : record.sv_end,
            'meta_var_type'         : var_type_ID,
            'meta_var_subtype'      : var_subtype_ID,
            'sample_1_called'       : rec_samples_0['called'],
            'sample_1_DP'           : rec_samples_0['data']['DP'],
            'sample_1_GQ'           : rec_samples_0['data']['GQ'],
            'sample_1_GT'           : rec_samples_0['data']['GT'],
            'sample_1_PL_1'         : rec_samples_0['data']['PL'][0],
            'sample_1_PL_2'         : rec_samples_0['data']['PL'][1],
            'sample_1_PL_3'         : rec_samples_0['data']['PL'][2],
            'sample_1_gt_alleles_1' : rec_samples_0['gt_alleles'][0],
            'sample_1_gt_alleles_2' : rec_samples_0['gt_alleles'][1],
            'sample_1_gt_type'      : rec_samples_0['gt_type'],
            'sample_1_is_het'       : rec_samples_0['is_het'],
            'sample_1_is_variant'   : rec_samples_0['is_variant'],
            'sample_1_phased'       : rec_samples_0['phased'],
            'sample_1_name'         : samples_names.index( rec_samples_0['sample'] ),
        }

        pos            = ((regs-1) % dumpevery)
        records[ pos ] = data

        if pos == (dumpevery - 1):
            #print "DUMPING"
            #http://stackoverflow.com/questions/11769366/why-is-sqlalchemy-insert-with-sqlite-25-times-slower-than-using-sqlite3-directly
            #engine.execute(
            #    Customer.__table__.insert(),
            #    [{"name":'NAME ' + str(i)} for i in range(n)]
            #)

            execute(
                Coords.__table__.insert(),
                records
            )
            #records = [None] * dumpevery


        if (debug > 0 and debug < 4 ) and ( debug > 5 and regs % (debug/5) == 0 ) or ( debug < 0 and regs % printevery == 0 ):
            #sys.stdout.write( str( pos ) + " " )
            diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
            regsLap      = 0
            startTimeLap = time.time()


        if regs == debug:
            break

    diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )

    pos = (regs-1) % dumpevery
    if regs % dumpevery != 0:
        data = records[:pos+1]
        print "last pos", pos, 'data', len(data   )
        execute(
            Coords.__table__.insert(),
            data
        )
        del data
    else:
        print "no remaining"

    del records


        #rec_meta = {
        #    #'genotype': genotype("name")           # Lookup a _Call for the sample given in name
        #    #get_hets: get_hets()                   # The list of het genotypes
        #    #get_hom_alts: get_hom_alts()           # The list of hom alt genotypes
        #    #get_hom_refs: get_hom_refs()           # The list of hom ref genotypes
        #    #get_unknowns: get_unknowns()           # The list of unknown genotypes
        #    #'samples'       : record.samples,        # list of _Calls for each sample ordered as in source VCF
        #    'aaf'           : record.aaf,            # A list of allele frequencies of alternate alleles. NOTE: Denominator calc?ed from _called_ genotypes.
        #    'call_rate'     : record.call_rate,      # The fraction of genotypes that were actually called.
        #    'end'           : record.end,            # 1-based end coordinate
        #    'heterozygosity': record.heterozygosity, # Heterozygosity of a site. Heterozygosity gives the probability that two randomly chosen chromosomes from the population have different alleles, giving a measure of the degree of polymorphism in a population. If there are i alleles with frequency p_i, H=1-sum_i(p_i^2)
        #    'is_deletion'   : record.is_deletion,    # Return whether or not the INDEL is a deletion
        #    'is_indel'      : record.is_indel,       # Return whether or not the variant is an INDEL
        #    'is_monomorphic': record.is_monomorphic, # Return True for reference calls
        #    'is_snp'        : record.is_snp,         # Return whether or not the variant is a SNP
        #    'is_sv'         : record.is_sv,          # Return whether or not the variant is a structural variant
        #    'is_sv_precise' : record.is_sv_precise,  # Return whether the SV cordinates are mapped to 1 b.p. resolution.
        #    'is_transition' : record.is_transition,  # Return whether or not the SNP is a transition
        #    'nucl_diversity': record.nucl_diversity, # pi_hat (estimation of nucleotide diversity) for the site. This metric can be summed across multiple sites to compute regional nucleotide diversity estimates. For example, pi_hat for all variants in a given gene.
        #    'num_called'    : record.num_called,     # The number of called samples
        #    'num_het'       : record.num_het,        # The number of heterozygous genotypes
        #    'num_hom_alt'   : record.num_hom_alt,    # The number of homozygous for alt allele genotypes
        #    'num_hom_ref'   : record.num_hom_ref,    # The number of homozygous for ref allele genotypes
        #    'num_unknown'   : record.num_unknown,    # The number of unknown genotypes
        #    'start'         : record.start,          # 0-based start coordinate
        #    'sv_end'        : record.sv_end,         # Return the end position for the SV
        #    'var_subtype'   : record.var_subtype,    # Return the subtype of variant. - For SNPs and INDELs, yeild one of: [ts, tv, ins, del] - For SVs yield either ?complex? or the SV type defined in the ALT fields (removing the brackets). E.g.:
        #    'var_type'      : record.var_type        # Return the type of variant [snp, indel, unknown] TO DO: support SVs
        #    'alleles'       : parseval( record.alleles )        # list of alleles. [0] = REF, [1:] = ALTS
        #}

        #http://pyvcf.readthedocs.org/en/latest/API.html#vcf-model-record


def parseval( val ):
    if repr(type(val)) == "<type 'list'>":
        res = []
        for e in val:
            #print e, type(e)
            res.append( str(e) )
        return res
    else:
        return val


def sample2dict( samples ):
    #print samples

    res = []
    for sample in samples:
        re = {
            'called'    : sample.called,     # True if the GT is not ./.
            'gt_alleles': sample.gt_alleles, # The numbers of the alleles called at a given sample
            'gt_bases'  : sample.gt_bases,   # The actual genotype alleles. E.g. if VCF genotype is 0/1, return A/G
            'gt_type'   : sample.gt_type,    # The type of genotype. hom_ref = 0 het = 1 hom_alt = 2 (we don;t track _which+ ALT) uncalled = None
            'is_het'    : sample.is_het,     # Return True for heterozygous calls
            'is_variant': sample.is_variant, # Return True if not a reference call
            'phased'    : sample.phased,     # A boolean indicating whether or not the genotype is phased for this sample
            'sample'    : sample.sample,     # The sample name
            #'site'      : sample.site,       # The _Record for this _Call
        }

        re['data'] = calldata2dict( sample.data ) # Dictionary of data from the VCF file

        res.append( re )

    return res


def calldata2dict( data ):
    res = {}

    for field in data._fields:
        res[ field ] = getattr(data, field)

    #pp.pprint( data )
    #pp.pprint( res  )

    return res




#{'ALT': ['G'],
# 'CHROM': 'SL2.40ch00',
# 'FILTER': None,
# 'FORMAT': 'GT:PL:DP:GQ',
# 'ID': None,
# 'INFO': {'AF1': 1.0,
#          'CI95': [1.0, 1.0],
#          'DP': 38,
#          'DP4': [0, 0, 19, 19],
#          'FQ': -141.0,
#          'MQ': 60},
# 'POS': 3235,
# 'QUAL': 222,
# 'REF': 'A',
# '_id': '/home/assembly/docker/vcf2sqlite/data/RF_001_SZAXPI008746-45.vcf.gz%09SL2.40ch00%09000000003235',
# 'nfo': {'aaf': [1.0],
#         'alleles': ['A', 'G'],
#         'call_rate': 1.0,
#         'end': 3235,
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
#         'get_hom_refs': [],
#         'get_unknowns': [],
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
#         'start': 3234,
#         'sv_end': None,
#         'var_subtype': 'ts',
#         'var_type': 'snp'},
# 'parent': '/home/assembly/docker/vcf2sqlite/data/RF_001_SZAXPI008746-45.vcf.gz'}
#{'ALT': ['C'],
# 'CHROM': 'SL2.40ch00',
# 'FILTER': None,
# 'FORMAT': 'GT:PL:DP:GQ',
# 'ID': None,
# 'INFO': {'AF1': 1.0,
#          'CI95': [1.0, 1.0],
#          'DP': 31,
#          'DP4': [0, 0, 15, 11],
#          'FQ': -105.0,
#          'MQ': 60},
# 'POS': 4314,
# 'QUAL': 222,
# 'REF': 'A',
# '_id': '/home/assembly/docker/vcf2sqlite/data/RF_001_SZAXPI008746-45.vcf.gz%09SL2.40ch00%09000000004314',
# 'nfo': {'aaf': [1.0],
#         'alleles': ['A', 'C'],
#         'call_rate': 1.0,
#         'end': 4314,
#         'get_hets': [],
#         'get_hom_alts': [{'called': True,
#                           'data': {'DP': 26,
#                                    'GQ': 99,
#                                    'GT': '1/1',
#                                    'PL': [255, 78, 0]},
#                           'gt_alleles': ['1', '1'],
#                           'gt_bases': 'C/C',
#                           'gt_type': 2,
#                           'is_het': False,
#                           'is_variant': True,
#                           'phased': False,
#                           'sample': '/panfs/ANIMAL/group001/minjiumeng/tomato_reseq/SZAXPI008746-45'}],
#         'get_hom_refs': [],
#         'get_unknowns': [],
#         'heterozygosity': 0.0,
#         'is_deletion': False,
#         'is_indel': False,
#         'is_monomorphic': False,
#         'is_snp': True,
#         'is_sv': False,
#         'is_sv_precise': False,
#         'is_transition': False,
#         'nucl_diversity': 0.0,
#         'num_called': 1,
#         'num_het': 0,
#         'num_hom_alt': 1,
#         'num_hom_ref': 0,
#         'num_unknown': 0,
#         'samples': [{'called': True,
#                      'data': {'DP': 26,
#                               'GQ': 99,
#                               'GT': '1/1',
#                               'PL': [255, 78, 0]},
#                      'gt_alleles': ['1', '1'],
#                      'gt_bases': 'C/C',
#                      'gt_type': 2,
#                      'is_het': False,
#                      'is_variant': True,
#                      'phased': False,
#                      'sample': '/panfs/ANIMAL/group001/minjiumeng/tomato_reseq/SZAXPI008746-45'}],
#         'start': 4313,
#         'sv_end': None,
#         'var_subtype': 'tv',
#         'var_type': 'snp'},
# 'parent': '/home/assembly/docker/vcf2sqlite/data/RF_001_SZAXPI008746-45.vcf.gz'}



if __name__ == "__main__":
    main( sys.argv[1:] )
