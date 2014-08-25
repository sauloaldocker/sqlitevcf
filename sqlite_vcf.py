#!/usr/bin/python
import sys
import os
import pprint
import argparse
import time
import copy

from multiprocessing        import Pool, Manager
#from multiprocessing.queues import SimpleQueue
from itertools import izip
from collections import OrderedDict, defaultdict

import vcf

import datetime

sys.path.insert(0, '.')
from database import *

"""
TODO:
	update index variables from database
	add snpeff
"""

SINGLE_THREADED = False
NUM_THREADS     = 1

#dumpevery       =  6400
#debug           = dumpevery * 2 # -1 no; > 1 = delete database, number of samples to read

#dumpevery       = 5
#debug           = dumpevery * 2 # -1 no; > 1 = delete database, number of samples to read

dumpevery       = 100000
debug           =    -1 # -1 no; > 1 = delete database, number of samples to read

printevery      = dumpevery * 3

sql_echo        = False



ppp             = pprint.PrettyPrinter(indent=1)
pp              = ppp.pprint

coord_num       = 0

startTime       = time.time()

eff_keys        = None

processing      = False

def main(args):
    print args
    indb    = args[0 ]
    infiles = args[1:]
    db      = loaddb(indb, echo=sql_echo)

    print "droping indexes"
    indexes        = db.list_indexes( table_name='coords' )
    db.drop_indexes(table_name='coords')


    file_IDs        = {}
    session         = db.get_session()
    metadata        = defaultdict(dict)

    print infiles

    for infile in infiles:
        print "processing file header", infile
        infile_abs_path    = os.path.abspath(  infile )
        infile_basename    = os.path.basename( infile )
        fileReg            = Files(file_path=infile_abs_path, file_base=infile_basename, file_name=infile)

        ifhd               = open(infile, 'r')
        vcf_reader         = vcf.Reader(ifhd)

        add_header( session, vcf_reader, fileReg )

        file_IDs[ infile ] = fileReg.file_ID

        del vcf_reader
        ifhd.close()


    if not SINGLE_THREADED:
        pool    = Pool( processes=NUM_THREADS )
        manager = Manager()
        queue   = manager.Queue()
        procs   = []

        for infile in infiles:
            print "adding thread to", infile
            proc    = pool.apply_async(process_file_multi, (queue, infile))
            procs.append( [ infile, proc ] )

        pool.close()


        while True:
            running = 0
            for procnum in xrange(len(procs)):
                infile, proc = procs[procnum]
                if proc:
                    running += 1
                    if proc.ready():
                        if proc.successful():
                            print "process %s has finished" % infile
                            r = proc.get()
                            print "res", r
                            procs[ procnum ][ 1 ] = None

                            process_q( queue, db, session, metadata, file_IDs, infile, running )
                        else:
                            print "error processing file", infile
                            r = proc.get()
                            print "res", r
                            pool.terminate()
                            sys.exit(1)

            if queue.empty():
                #print "queue empty.", running, "running"
                if running == 0:
                    while processing:
                        print "no running thread. waiting processing to finish"
                        time.sleep(5)

                    print "no running thread. finished"
                    break
                time.sleep(1)

            else:
                process_q( queue, db, session, metadata, file_IDs, infile, running )

    else:
        processor = get_process_records_single(db, session, file_IDs, metadata)
        for infile in infiles:
            process_file( infile, processor )

    process_metadata( db, session, metadata )

    print "adding indexes"
    db.add_indexes(indexes, 'coords')
    print "finished"


def process_q( queue, db, session, metadata, file_IDs, infile, running ):
    print "queue not empty.", running, "still running.", NUM_THREADS, 'threads.', queue.qsize(),"in queue. whiling"
    while not queue.empty():
        print "queue not empty.", running, "still running.", NUM_THREADS, 'threads.', queue.qsize(),"in queue"
        infile, data = queue.get_nowait()
        print "got data from file", infile
        file_ID      = file_IDs[ infile ]
        #pp( data )
        process_records( db, session, infile, file_ID, metadata, data )
        time.sleep(0.5)


def diffTime( runid, chrom, startTimeFile, startTimeChrom, startTimeLap, regs, regsChrom, regsLap ):
    currTime       = time.time()

    diffTimeStart  = currTime - startTime
    diffTimeFile   = currTime - startTimeFile
    diffTimeChrom  = currTime - startTimeChrom
    diffTimeLap    = currTime - startTimeLap

    diffRegsStart  = coord_num
    diffRegsFile   = regs
    diffRegsChrom  = regsChrom
    diffRegsLap    = regsLap

    speedStart     = diffRegsStart  / diffTimeStart
    speedFile      = diffRegsFile   / diffTimeFile
    speedChrom     = diffRegsChrom  / diffTimeChrom
    speedLap       = diffRegsLap    / diffTimeLap

    print "\
%s :: Time   : Start %7ds  File %7ds      Chrom %s %7ds      Lap %7ds\n\
%s :: Records: Start %7d   File %7d       Chrom %s %7d       Lap %7d\n\
%s :: Speed  : Start %7d   File %7d rec/s Chrom %s %7d rec/s Lap %7d rec/s\n\n" % (  \
        runid, diffTimeStart, diffTimeFile, chrom, diffTimeChrom, diffTimeLap,
        runid, diffRegsStart, diffRegsFile, chrom, diffRegsChrom, diffRegsLap,
        runid, speedStart   , speedFile   , chrom, speedChrom   , speedLap )


def get_process_records_single( db, session, file_IDs, metadata ):
    def processor(infile, data):
        process_records(db, session, infile, file_IDs[infile], metadata, data)

    return processor


def get_process_records_multi( q ):
    def processor(infile, data):
        try:
            q.put_nowait( ( infile, data ) )
        except Exception as inst:
            print "ERROR PUTTING TO QUEUE"
            print type(inst)     # the exception instance
            print inst.args      # arguments stored in .args
            print inst           # __str__ allows args to be printed directly
            raise

    return processor


def process_records( db, session, infile, file_ID, metadata, records ):
    execute         = db.engine.execute
    ins             = Coords.__table__.insert()
    global processing
    processing      = True

    #pp( records )

    chromposes      = metadata['chromposes']
    formats         = metadata['formats'   ]
    refs            = metadata['refs'      ]
    alts            = metadata['alts'      ]
    types           = metadata['types'     ]
    subtypes        = metadata['subtypes'  ]
    chroms          = metadata['chroms'    ]
    get_or_update   = db.get_or_update

    global coord_num
    print infile, "COORD INITIAL", coord_num, 'appending', len(records)

    for data in records:
        coord_num     += 1

        pos            = data['Pos']
        chrom_k        = ( ('chrom_name'       , data['chrom_ID']), )
        chrom_ID       = chroms.get(     chrom_k      , -1 )

        if chrom_ID == -1:
            chrom_ID          = len( chroms ) + 1
            chroms[ chrom_k ] = chrom_ID

        chrompos_k     = ( ('chrom_ID'       , chrom_ID                ), ('Pos', pos) )
        format_k       = ( ('format_str'     , data['format_ID'       ]), )
        ref_k          = ( ('ref_str'        , data['ref_ID'          ]), )
        alt_k          = ( ('alt_str'        , data['alt_ID'          ]), )
        var_type_k     = ( ('var_type_str'   , data['meta_var_type'   ]), )
        var_subtype_k  = ( ('var_subtype_str', data['meta_var_subtype']), )

        chrompos_ID    = chromposes.get( chrompos_k   , -1 )
        format_ID      = formats.get(    format_k     , -1 )
        ref_ID         = refs.get(       ref_k        , -1 )
        alt_ID         = alts.get(       alt_k        , -1 )
        var_type_ID    = types.get(      var_type_k   , -1 )
        var_subtype_ID = subtypes.get(   var_subtype_k, -1 )

        if chrompos_ID    == -1:
            chrompos_ID              = len( chromposes ) + 1
            chromposes[ chrompos_k ] = chrompos_ID

        if format_ID      == -1:
            format_ID                = len( formats ) + 1
            formats[ format_k ]      = format_ID

        if ref_ID         == -1:
            ref_ID        = len( refs ) + 1
            refs[ ref_k ] = ref_ID

        if alt_ID         == -1:
            alt_ID        = len( alts ) + 1
            alts[ alt_k ] = alt_ID

        if var_type_ID    == -1:
            var_type_ID         = len( types ) + 1
            types[ var_type_k ] = var_type_ID

        if var_subtype_ID == -1:
            var_subtype_ID            = len( subtypes ) + 1
            subtypes[ var_subtype_k ] = var_subtype_ID

        data['coord_ID'        ] = coord_num
        data['file_ID'         ] = file_ID
        data['chrom_ID'        ] = chrom_ID
        data['format_ID'       ] = format_ID
        data['ref_ID'          ] = ref_ID
        data['alt_ID'          ] = alt_ID
        data['chrompos_ID'     ] = chrompos_ID
        data['meta_var_type'   ] = var_type_ID
        data['meta_var_subtype'] = var_subtype_ID

    execute(
        ins,
        records
    )

    session.commit()
    session.flush()

    print infile, "COORD FINAL  ", coord_num
    processing      = False


def process_file_multi( q, infile ):
    processor = get_process_records_multi(q)
    process_file( infile, processor )


def process_metadata( db, session, metadata ):
    execute         = db.engine.execute

    tables = (
        ( 'chroms'    , 'chrom_ID'      , Chroms     ),
        ( 'chromposes', 'chrompos_ID'   , ChromPos   ),
        ( 'formats'   , 'format_ID'     , Format_col ),
        ( 'refs'      , 'ref_ID'        , Refs       ),
        ( 'alts'      , 'alt_ID'        , Alts       ),
        ( 'types'     , 'var_type_ID'   , VarType    ),
        ( 'subtypes'  , 'var_subtype_ID', VarSubType )
    )

    for table_name, key_id_name, table in tables:
        data      = metadata[ table_name ]
        #print "TABLE NAME", table_name, "KEY ID NAME", key_id_name, "LEN", len(data), data
        ins       = table.__table__.insert()
        registers = [None] * len(data)
        reg_count = 0

        for val_tuple in data:
            #print "  REG", reg_count
            #print "    VAL TUPLE", val_tuple
            val_dict   = dict( (x, y) for x, y in val_tuple )
            #print "    VAL DICT ", val_dict
            data_num = data[ val_tuple ]
            #print "    DATA NUM ", data_num
            val_dict[ key_id_name ] = data_num
            #print "    VAL DICT ", val_dict
            registers[ reg_count  ] = val_dict
            reg_count += 1

        print "INSERTING", table_name
        execute( ins, registers )
        print "INSERTED ", table_name
    session.commit()
    session.flush()


def add_header( session, vcf_reader, fileReg ):
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
                    if el_name == 'EFF':
                        global eff_keys
                        #print 'EFF', el_val._asdict()
                        vals     = el_val._asdict()
                        desc     = vals['desc']
                        desc     = desc[desc.find("'")+1:].replace("'", "")
                        #print 'DESC', desc
                        desc     = desc.replace('[', '').replace(']', '').replace(')', '').replace('(', '|').strip().split( '|' )
                        desc     = [ x.strip() for x in desc ]
                        #print 'DESC LIST', desc
                        eff_keys = desc
                        #for k in eff_keys:
                        #    print 'EFF K', k, 'VAL', eff_keys[k]

                        ##infos['EFF']['desc'] = [ x. for x in infos['EFF']['desc'] ]
                        #Spos = eff_keys['EFF']['desc'].find('Format: ')
                        #if Spos != -1:
                        #    Spos += 8
                        #    eff_keys['EFF']['format'] = eff_keys['EFF']['desc'][Spos:]
                        #    eff_keys['EFF']['format'] = eff_keys['EFF']['format'].replace('[', '').replace(']', '').replace(')', '').replace('\'', '').replace('(', '|').split( '|' )
                        #    eff_keys['EFF']['format'] = [ x.strip() for x in eff_keys['EFF']['format'] ]
                        #    #print 'INFOS EFF FORMAT', infos['EFF']['format']
                        #print eff_keys

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


def parseval( val ):
    if repr(type(val)) == "<type 'list'>":
        res = []
        for e in val:
            #print e, type(e)
            res.append( str(e) )
        return res
    else:
        return val


def process_file( infile, saver ):
    print "processing", infile
    sys.stdin.close()

    if not os.path.exists( infile ):
        print "input file %s does not exists" % infile
        sys.exit(1)

    colnames        = ('CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT' )

    try:
        infile_fhd      = open(infile, 'r')
    except:
        print '!'*50
        print "ERROR OPENING FILE", infile
        print '!'*50
        raise

    vcf_reader      = vcf.Reader( infile_fhd )

    samples_names   = getattr(vcf_reader, 'samples', [])

    lastChrom       = None
    lastChromUrl    = None
    lastPos         = None
    regs            = 0
    regsChrom       = 0
    regsLap         = 0
    startTimeFile   = time.time()
    startTimeChrom  = startTimeFile
    startTimeLap    = startTimeFile
    records         = [None] * dumpevery

    for record in vcf_reader:
        #print pp.pprint( record )
        regs      += 1
        regsChrom += 1
        regsLap   += 1

        if regs == debug:
            break

        rec = {}
        for k in colnames:
            val = getattr( record, k )
            #print k, val
            rec[ k ] = parseval( val )

        chrom = rec[ 'CHROM' ]
        pos   = rec[ 'POS'   ]

        #pp(rec)

        alt_str        = ','.join(rec['ALT'   ])

        rec_samples   = []
        for sample in record.samples:
            res = {
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

            res_data = {} # Dictionary of data from the VCF file
            for field in sample.data._fields:
                res_data[ field ] = getattr(sample.data, field)
            res['data'] = res_data
            rec_samples.append( res )


        rec_samples_0 = rec_samples[0]
        if len( rec_samples ) != 1:
            print "more than one sample"
            sys.exit(1)


        rec_info     = rec['INFO']


        data = {
            #'coord_ID'              : coord_num,
            #'file_ID'               : file_ID,
            #'chrom_ID'              : chrom_ID,
            #'format_ID'             : format_ID,
            #'ref_ID'                : ref_ID,
            #'alt_ID'                : alt_ID,
            #'chrompos_ID'           : chrompos_ID,

            'coord_ID'              : None,
            'file_ID'               : None,
            'chrom_ID'              : rec['CHROM'],
            'format_ID'             : rec['FORMAT'],
            'ref_ID'                : rec['REF'],
            'alt_ID'                : alt_str,
            'chrompos_ID'           : None,
            'meta_var_type'         : record.var_type,
            'meta_var_subtype'      : record.var_subtype,

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

            "eff_Effect"            : None,
            "eff_Effect_Impact"     : None,
            "eff_Functional_Class"  : None,
            "eff_Codon_Change"      : None,
            "eff_Amino_Acid_change" : None,
            "eff_Amino_Acid_length" : None,
            "eff_Gene_Name"         : None,
            "eff_Gene_BioType"      : None,
            "eff_Coding"            : None,
            "eff_Transcript"        : None,
            "eff_Exon"              : None,
            "eff_GenotypeNum"       : None,
            "eff_ERRORS"            : None,
            "eff_WARNINGS"          : None
        }

        #fix SNPeff
        if 'EFF' in rec_info:
            eff_data = rec_info['EFF'][0]
            #print "INFO EFF B", eff_data
            eff_data = eff_data.replace(')', '').replace('(', '|').split( '|' )
            eff_data = [ x.strip() for x in eff_data ]
            #print "INFO EFF A", eff_data
            #print len(eff_data), len(eff_keys)
            #print eff_data
            #print eff_keys

            if len(eff_data) != len(eff_keys):
                if len(eff_data) == (len(eff_keys)-2):
                    eff_data += ['', '']
                    #print "INFO EFF C", effdata
                elif len(eff_data) == (len(eff_keys)-1):
                    eff_data += ['']
                else:
                    print "NUMBER OF SNPEFF ROWS != NUMBER OF SNPEFF TITLES", len(eff_data), len(eff_keys)
                    print eff_data
                    print eff_keys
                    regs -= 1
                    continue
                    #sys.exit(1)

            for e in xrange(len(eff_keys)):
                ek = 'eff_' + eff_keys[e]
                ev =          eff_data[e]
                #print ek
                data[ ek ] = ev


        #pp( data )


        pos            = ((regs-1) % dumpevery)
        records[ pos ] = data

        if pos == (dumpevery - 1):
            #http://stackoverflow.com/questions/11769366/why-is-sqlalchemy-insert-with-sqlite-25-times-slower-than-using-sqlite3-directly
            print "processing", infile, "pos", pos, 'data', len( records ), 'sending'
            try:
                saver( infile, records )
            except Exception as inst:
                print "ERROR SENDING TO SAVER"
                print type(inst)     # the exception instance
                print inst.args      # arguments stored in .args
                print inst           # __str__ allows args to be printed directly
                raise
            print "processing", infile, "pos", pos, 'data', len( records ), 'sent'

        if (debug > 0 and debug <= 4 ) or ( debug > 5 and regs % (debug/5) == 0 ) or ( debug < 0 and regs % printevery == 0 ):
            diffTime( infile, chrom, startTimeFile, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
            regsLap      = 0
            startTimeLap = time.time()



    pos = (regs-1) % dumpevery
    if regs % dumpevery != 0:
        data = records[:pos+1]
        print "processing", infile, "pos", pos, 'data', len( data ), 'LAST. sending'
        try:
            saver( infile, data )
        except Exception as inst:
            print "ERROR SENDING TO SAVER LAST"
            print type(inst)     # the exception instance
            print inst.args      # arguments stored in .args
            print inst           # __str__ allows args to be printed directly
            raise
        print "processing", infile, "pos", pos, 'data', len( data ), 'LAST. sent'

    else:
        print "processing", infile, "no remaining"

    diffTime( infile, chrom, startTimeFile, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
    print "processing", infile, "no remaining", 'RETURNING'


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
