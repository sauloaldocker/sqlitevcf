#!/usr/bin/python
#http://api.mongodb.org/python/current/tutorial.html
import sys
import os
import pprint
import argparse
import time
import gc
from collections import OrderedDict, defaultdict

import vcf

import datetime

sys.path.insert(0, '.')
import database


#docker run -i -t --rm -v $PWD/data/:/data:rw -v $PWD/db:/db sauloal/vcflite pypy db/sqlite_vcf.py -ri -rm -d db/RF_002_SZAXPI009284-57.vcf.gz.data.sqlite -i db/RF_002_SZAXPI009284-57.vcf.gz.index.sqlite data/RF_002_SZAXPI009284-57.vcf.gz
#time find data/ -name 'RF_*.vcf.gz' | xargs -I{} -r -P20 bash -c "VCF={}; bn=\`basename {}\`; of=db/\$bn.data.sqlite; pf=db/\$bn.index.sqlite; rm -f $of; rm -f $pf; number=\$RANDOM; RANGE=30; let 'number %= $RANGE'; sleep \$number; echo \$VCF \$of \$pf \$number; docker run -i -t --rm -v \$PWD/data/:/data:rw -v \$PWD/db:/db sauloal/vcflite pypy db/sqlite_vcf.py -ri -rm -d \$of -i \$pf \$VCF"
#run -i -t --rm -v $PWD/data/:/data:rw -v $PWD/db:/db sauloal/vcflite pypy db/sqlite_vcf.py -MERGE -ri -rm -d db/merged.sqlite db/RF_00*data.sqlite

#84 w/ index
#real    204m18.846s
#user    0m3.371s
#sys     0m2.274s


#ADD 3h
#84h 14min
#real    1094m9.772s
#user    0m0.849s
#sys     0m0.280s


sql_echo        = True
sql_echo        = False

#printevery      =  6400
#dumpevery       =  6400
#debug           = 12800 # -1 no; > 1 = delete database, number of samples to read

#printevery      =  5
#dumpevery       =  5
#debug           = 10 # -1 no; > 1 = delete database, number of samples to read

printevery      =  1000000
dumpevery       =  1000000
debug           =       -1 # -1 no; > 1 = delete database, number of samples to read


ppp = pprint.PrettyPrinter(indent=1)
pp  = ppp.pprint

gc.enable()



def main(args):
    parser = argparse.ArgumentParser(description='Convert VCF to SQLite database')
    parser.add_argument('-d'     , '--data' , nargs='?', dest='out_data'       , default=None, type=str, help='Save Data database')
    parser.add_argument('-i'     , '--index', nargs='?', dest='out_index'      , default=None, type=str, help='Save Index database')
    parser.add_argument('-m'     , '--meta' , nargs='?', dest='out_meta'       , default=None, type=str, help='Save Metadata database')

    parser.add_argument('-MERGE' , '--JUST-MERGE'      , dest='merge'          , action='store_true', help='Just merge databases')
    parser.add_argument('-ri'    , '--restore-indexes' , dest='restore_indexes', action='store_true', help='Restore indexes after insertion')
    parser.add_argument('-rm'    , '--remove'          , dest='remove'         , action='store_true', help='Remove output files if they exists')

    parser.add_argument('infiles',            nargs=argparse.REMAINDER)

    args = parser.parse_args(args)

    if len(args.infiles) == 0:
        print "no input files"
        print args
        parser.print_help()
        parser.print_usage()
        sys.exit(1)


    if args.merge:
        do_merge(args)
        return

    if not any([args.out_data, args.out_index, args.out_meta]):
        print "no output files"
        print args
        parser.print_help()
        parser.print_usage()
        sys.exit(1)

    if args.remove:
        for of in [args.out_data, args.out_index, args.out_meta]:
            if of is None:
                continue
            if os.path.exists(of):
                print "deleting", of
                os.remove(of)

    gstart  = time.time()
    print args

    db              = database.loaddb(db_data_name=args.out_data, db_index_name=args.out_index, db_meta_name=args.out_meta, echo=sql_echo)

    available_dbs   = db.list_dbs()
    print "available databases", available_dbs

    if 'data'in available_dbs:
        print "droping indexes"
        indexes        = db.list_indexes( 'data', table_name='coords' )
        db.drop_indexes( 'data', table_name='coords' )

        ptime = time.time()
        print "processing files", len(args.infiles)
        print args.infiles

        metadata = defaultdict(OrderedDict)

        for infile in args.infiles:
            process_file( db, args, metadata, infile )

        print "files processed", time.time() - ptime

        if 'meta' in available_dbs:
            process_metadata(db, metadata)

        if args.restore_indexes:
            itime = time.time()
            print "adding indexes"
            db.add_indexes( 'data', indexes, table_name='coords' )
            print "finished", time.time() - itime

        open(args.out_data + '.ok', 'w').write('ok')


    #session.close()

    tdiff = time.time() - gstart
    print "total time      %10d s"      % ( tdiff                                )

    if 'data' in available_dbs:
        print "total registers %10d regs"   % ( metadata['reg_ids']['count']         )
        print "average speed   %10d regs/s" % ( metadata['reg_ids']['count'] / tdiff )


def do_merge(args):
    outfile = args.out_data
    infiles = args.infiles

    #cp RF_001_SZAXPI008746-45.vcf.gz.data.sqlite data1.sqlite
    #echo "SELECT name FROM sqlite_master WHERE type == 'index';" | sqlite3 data1.sqlite | perl -ne 'chomp; print "DROP INDEX $_;\n"' | sqlite3 data1.sqlite
    #time echo .dump | sqlite3 RF_002_SZAXPI009284-57.vcf.gz.data.sqlite | grep -v 'CREATE INDEX' | sqlite3 data1.sqlite
    #real    0m47.346s
    #user    0m57.418s
    #sys     0m 2.288s


    #cp RF_001_SZAXPI008746-45.vcf.gz.data.sqlite data2.sqlite
    #echo "SELECT name FROM sqlite_master WHERE type == 'index';" | sqlite3 data2.sqlite | perl -ne 'chomp; print "DROP INDEX $_;\n"' | sqlite3 data2.sqlite
    #time echo "attach 'RF_002_SZAXPI009284-57.vcf.gz.data.sqlite' as toMerge;
    #BEGIN;
    #insert into coords select * from toMerge.coords;
    #COMMIT;
    #detach database toMerge;" | sqlite3 data2.sqlite
    #real    0m5.590s
    #user    0m5.010s
    #sys     0m0.505s

    #echo .sch | sqlite3 RF_001_SZAXPI008746-45.vcf.gz.data.sqlite | sqlite3 data2.sqlite

    print "MERGING to", outfile

    if args.remove:
        if os.path.exists( outfile ):
            print "removing outfile"
            os.remove( outfile )

    for infile in infiles:
        if not os.path.exists(infile):
            print "input file %s does not exists" % infile
            sys.exit(1)

        if not infile.endswith('.sqlite'):
            print "input file %s is not a .sqlite file" % infile
            sys.exit(1)

    if len(infiles) < 2:
        print "less than two files"
        print infiles
        sys.exit(1)

    firstFile = infiles.pop(0)


    loop   = ""
    for infile in infiles:
        loop += """

echo "ADDING %(infile)s"
time echo "attach '%(infile)s' as toMerge;
BEGIN;
insert into coords select * from toMerge.coords;
COMMIT;
detach database toMerge;" | sqlite3 %(outFile)s
echo "ADDED %(infile)s"

""" % { 'infile': infile, 'outFile': outfile }



    create_index = ""
    if args.restore_indexes:
        create_index = """

echo "RE CREATING INDEX"
time echo .sch | sqlite3 %(firstFile)s | grep "CREATE INDEX " | perl -ne 'BEGIN { print "BEGIN;\\n"; } END { print "COMMIT;\\n";  }; print' | sqlite3 %(outFile)s
echo "FINISHED RE CREATING INDEX"

""" %   {
            'firstFile': firstFile,
            'outFile' : outfile
        }



    script = """
echo "COPYING FIRST"
time cp %(firstFile)s %(outFile)s
echo "FIRST FILE COPIED"


echo "DROPPING INDEX"
time echo "SELECT name FROM sqlite_master WHERE type == 'index';" | sqlite3 %(outFile)s | perl -ne 'BEGIN { print "BEGIN;\\n"; } END { print "COMMIT;\\n";  }; chomp; print "DROP INDEX $_;\\n"' | sqlite3 %(outFile)s
echo "INDEX DROPPED"


echo "LOOPING"
%(loop)s
echo "LOOPPED"


%(create_index)s
""" % {
        'firstFile'    : firstFile,
        'outFile'     : outfile,
        'loop'        : loop,
        'create_index': create_index
    }

    osh = outfile+'.create.sh'
    print "BASH CREATED", osh
    open(osh, 'w').write(script)

    print "NOW RUN:", osh
    #print "RUNNING"
    #import subprocess
    #subprocess.call('/bin/bash '+ osh, shell=True)
    #print "FINISHED"


def process_metadata( db, metadata ):
    session         = db.get_session('meta')
    engine          = db.get_engine( 'meta')
    execute         = engine.execute

    tables = (
        #( 'chromposes', database.ChromPos   ),
        ( 'chroms'    , database.Chroms     ),
        ( 'formats'   , database.Format_col ),
        ( 'refs'      , database.Refs       ),
        ( 'alts'      , database.Alts       ),
        ( 'types'     , database.VarType    ),
        ( 'subtypes'  , database.VarSubType )
    )

    #session.execute("COMMIT")
    #session.execute("BEGIN TRANSACTION")
    igtime = time.time()
    print "inserting"
    for table_name, table in tables:
        itime     = time.time()
        data      = metadata[ table_name ]
        vals      = data.values()
        #print table_name, data
        #print "TABLE NAME", table_name, "KEY ID NAME", key_id_name, "LEN", len(data), data
        ins       = table.__table__.insert(inline=True)
        print "INSERTING", table_name, len(data)
        execute( ins, vals )
        print "INSERTED ", table_name, time.time() - itime
    print "inserted", time.time() - igtime

    #session.execute("END TRANSACTION")

    session.commit()
    session.flush()


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


def add_header(db, vcf_reader, infile):
    session         = db.get_session('index')
    #engine          = db.get_engine( 'index')
    #execute         = engine.execute

    infile_abs_path = os.path.abspath(  infile )
    infile_basename = os.path.basename( infile )

    fileReg         = database.Files(file_path=infile_abs_path, file_base=infile_basename, file_name=infile)
    session.add( fileReg )
    session.commit()
    session.flush()
    db.use('index')


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
                header = database.Header(header_name=header_key, header_value=header_val)
                headers['header'].append( header )
                #print "    APPENDED"



        elif header_type == 'list':
            #print "  ADDING LIST"
            for el in header_val:
                #print "  ADDING LIST EL", el
                header = database.Header(header_name=header_key, header_value=el)
                headers['header'].append( header )
                #print "    APPENDED"



        elif header_type == 'dict':
            #print "  ADDING DICT"
            for el_name in header_val:
                el_val = header_val[el_name]
                #print "  ADDING DICT EL_NAME", el_name
                #print "  ADDING DICT EL_VAL ", el_val

                if   header_key == 'formats':
                    header_format = database.Header_format(header_format_name=el_name, header_format_num=el_val[1], header_format_type=el_val[2], header_format_desc=el_val[3])
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


                    header_info = database.Header_info(header_info_name=el_name, header_info_num=el_val[1], header_info_type=el_val[2], header_info_desc=el_val[3])
                    headers['infos'].append( header_info )
                    #print "    APPENDED"

                elif header_key == 'metadata':
                    if   type(el_val) == type(list()):
                        for el_val_val in el_val:
                            header_meta = database.Header_meta(header_meta_name=el_name, header_meta_desc=el_val_val)
                            headers['metadata'].append( header_meta )
                            #print "    APPENDED"

                    elif type(el_val) == type(str()):
                        header_meta = database.Header_meta(header_meta_name=el_name, header_meta_desc=el_val)
                        headers['metadata'].append( header_meta )
                        #print "    APPENDED"

                else:
                    header = database.Header(header_name=header_key + ':' + el_name, header_value=el_val)
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


def process_file( db, args, metadata, infile ):
    istime = time.time()
    print "processing", infile

    #colnames        = ('CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT' )

    vcf_reader      = vcf.Reader(open(infile, 'r'))
    available_dbs   = db.list_dbs()
    if 'index' in available_dbs:
        add_header( db, vcf_reader, infile )




    if 'data' in available_dbs:
        infile_abs_path = os.path.abspath(  infile )

        session         = db.get_session('data')
        engine          = db.get_engine( 'data')
        execute         = engine.execute
        ins             = database.Coords.__table__.insert(inline=True)

        samples_names   = getattr(vcf_reader, 'samples', [])

        lastChrom       = None
        lastChromUrl    = None
        lastPos         = None
        regs            = 0
        regsChrom       = 0
        regsLap         = 0
        startTime       = time.time()
        startTimeChrom  = startTime
        startTimeLap    = startTime
        atime           = startTime

        records         = [None] * dumpevery

        reg_ids         = metadata['reg_ids' ]

        if 'count' not in reg_ids:
            reg_ids['count'] = 0

        reg_id          = reg_ids['count']

        for record in vcf_reader:
            regs      += 1
            regsChrom += 1
            regsLap   += 1


            #fix SNPeff
            #if 'INFO' in rec and 'EFF' in rec['INFO']:
            #    #print "INFO EFF B", rec['INFO']['EFF']
            #    rec['INFO']['EFF'] = [ x.replace(')', '').replace('(', '|').strip().split( '|' ) for x in rec['INFO']['EFF'] ]
            #    #print "INFO EFF A", rec['INFO']['EFF']
            #

            pos                  = record.POS
            chrom                = record.CHROM
            alt_str              = ','.join([str(x) for x in record.ALT])
            samples              = record.samples
            sample_0             = samples[0]
            sample_0_gt_alleles  = sample_0.gt_alleles
            sample_0_data        = {}

            for field in sample_0.data._fields:
                sample_0_data[ field ] = getattr(sample_0.data, field)

            sample_0_data_PL     = sample_0_data['PL']
            sample_0_data_PL_len = len(sample_0_data_PL)
            rec_info             = record.INFO
            rec_info_DP4         = rec_info['DP4' ]

            record_aaf           = record.aaf
            record_aaf_len       = len(record_aaf)


            assert len( samples           ) ==  1                                , "more than one sample %s" % str(samples)
            assert len(rec_info['CI95']   ) ==  2 if 'CI95' in rec_info else True, "not 2 CI95 %s"           % str(rec_info['CI95'])
            assert len(rec_info_DP4       ) ==  4                                , "not four DP4 %s"         % str(rec_info_DP4)
            assert     record_aaf_len       <=  3                                , "more than 3 aaf %s"      % str(record_aaf)
            assert     sample_0_data_PL_len <= 10                                , "not 10 PL %d %s"         % (sample_0_data_PL_len, str(sample_0_data_PL))
            assert len(sample_0_gt_alleles) ==  2                                , "not 2 alleles %s"        % str(sample_0_gt_alleles)


            data = {
                'file_path'             : infile_abs_path,
                'Chrom'                 : chrom,
                'Format'                : record.FORMAT,
                'Ref'                   : record.REF,
                'Alt'                   : alt_str,

                'Pos'                   : pos,
                'Qual'                  : record.QUAL,
                'Filter'                : record.FILTER,
                'Id'                    : record.ID,
                'info_AF1'              : rec_info['AF1' ],
                'info_CI95_1'           : rec_info['CI95'][0] if 'CI95' in rec_info else None,
                'info_CI95_2'           : rec_info['CI95'][1] if 'CI95' in rec_info else None,
                'info_DP'               : rec_info['DP'  ],
                'info_DP4_1'            : rec_info_DP4[0],
                'info_DP4_2'            : rec_info_DP4[1],
                'info_DP4_3'            : rec_info_DP4[2],
                'info_DP4_4'            : rec_info_DP4[3],
                'info_FQ'               : rec_info['FQ'  ],
                'info_MQ'               : rec_info['MQ'  ],
                'meta_aaf_1'            : record_aaf[0],
                'meta_aaf_2'            : record_aaf[1] if record_aaf_len > 1 else None,
                'meta_aaf_3'            : record_aaf[2] if record_aaf_len > 2 else None,
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

                'meta_var_type'         : record.var_type,
                'meta_var_subtype'      : record.var_subtype,

                'sample_1_DP'           : sample_0_data['DP'],
                'sample_1_GQ'           : sample_0_data['GQ'],
                'sample_1_GT'           : sample_0_data['GT'],
                'sample_1_PL_1'         : sample_0_data_PL[0],
                'sample_1_PL_2'         : sample_0_data_PL[1],
                'sample_1_PL_3'         : sample_0_data_PL[2],
                'sample_1_PL_4'         : sample_0_data_PL[3] if sample_0_data_PL_len > 3 else None,
                'sample_1_PL_5'         : sample_0_data_PL[4] if sample_0_data_PL_len > 4 else None,
                'sample_1_PL_6'         : sample_0_data_PL[5] if sample_0_data_PL_len > 5 else None,
                'sample_1_PL_7'         : sample_0_data_PL[6] if sample_0_data_PL_len > 6 else None,
                'sample_1_PL_8'         : sample_0_data_PL[7] if sample_0_data_PL_len > 7 else None,
                'sample_1_PL_9'         : sample_0_data_PL[8] if sample_0_data_PL_len > 8 else None,
                'sample_1_PL_10'        : sample_0_data_PL[9] if sample_0_data_PL_len > 9 else None,
                'sample_1_gt_alleles_1' : sample_0_gt_alleles[0], # The numbers of the alleles called at a given sample
                'sample_1_gt_alleles_2' : sample_0_gt_alleles[1],
                'sample_1_called'       : sample_0.called,     # True if the GT is not ./.
                'sample_1_gt_type'      : sample_0.gt_type,    # The type of genotype. hom_ref = 0 het = 1 hom_alt = 2 (we don;t track _which+ ALT) uncalled = None
                'sample_1_is_het'       : sample_0.is_het,     # Return True for heterozygous calls,
                'sample_1_is_variant'   : sample_0.is_variant, # Return True if not a reference call
                'sample_1_phased'       : sample_0.phased,     # A boolean indicating whether or not the genotype is phased for this sample
                'sample_1_name'         : samples_names.index( sample_0.sample ),
            }

            pos               = ((regs-1) % dumpevery)
            records[ pos   ]  = data
            reg_id           += 1

            if pos == (dumpevery - 1):
                diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
                itime = time.time()
                print "inserting"
                #db.use('data')
                #session.begin(subtransactions=True )
                #db.use('data')
                execute(
                    ins,
                    records
                )
                session.commit()
                session.flush()
                #db.use()

                #records = [None] * dumpevery
                print "inserted", time.time() - itime
                diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )
                regsLap      = 0
                startTimeLap = time.time()


            if regs == debug:
                break

        print "finished processing", infile, "commiting"
        diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )

        reg_ids['count'] = reg_id

        pos = (regs-1) % dumpevery
        if regs % dumpevery != 0:
            itime  = time.time()
            print "inserting final"
            del   records[pos+1:]
            ctime  = time.time()
            print "last pos", pos, 'data', len(records)
            #db.use()
            #session.execute('BEGIN TRANSACTION')
            #session.begin(subtransactions=True )
            #db.use()
            execute(
                ins,
                records
            )

            #session.execute('END TRANSACTION')
            session.commit()
            session.flush()
            #db.use()
            print "inserted final", time.time() - itime
        else:
            print "no remaining"

        #print gc.get_referrers(records)

        del records

        gc.collect()

        diffTime( infile, chrom, startTime, startTimeChrom, startTimeLap, regs, regsChrom, regsLap )

        session.close()

        print "finished processing", infile, time.time() - istime, "\n\n"



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


#def parseval( val ):
#    if repr(type(val)) == "<type 'list'>":
#        res = []
#        for e in val:
#            #print e, type(e)
#            res.append( str(e) )
#        return res
#    else:
#        return val


#def sample2dict( samples ):
#    #print samples
#
#    res = []
#    for sample in samples:
#        re = {
#            'called'    : sample.called,     # True if the GT is not ./.
#            'gt_alleles': sample.gt_alleles, # The numbers of the alleles called at a given sample
#            'gt_bases'  : sample.gt_bases,   # The actual genotype alleles. E.g. if VCF genotype is 0/1, return A/G
#            'gt_type'   : sample.gt_type,    # The type of genotype. hom_ref = 0 het = 1 hom_alt = 2 (we don;t track _which+ ALT) uncalled = None
#            'is_het'    : sample.is_het,     # Return True for heterozygous calls
#            'is_variant': sample.is_variant, # Return True if not a reference call
#            'phased'    : sample.phased,     # A boolean indicating whether or not the genotype is phased for this sample
#            'sample'    : sample.sample,     # The sample name
#            #'site'      : sample.site,       # The _Record for this _Call
#        }
#
#        re['data'] = calldata2dict( sample.data ) # Dictionary of data from the VCF file
#
#        res.append( re )
#
#    return res


#def calldata2dict( data ):
#    res = {}
#
#    for field in data._fields:
#        res[ field ] = getattr(data, field)
#
#    #pp.pprint( data )
#    #pp.pprint( res  )
#
#    return res




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
