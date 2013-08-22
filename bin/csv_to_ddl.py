#!/usr/bin/python
"""
    This script uses string manipulation to generate sql! 
    Mind what you feed it and be sure to check its outputs before executing them.
"""
import sys,argparse,os,csv,json
from flock import db
from flock.parsers import *
from flock import output,fancycsv,fancystring,eyeoh
from flock.ddl import TypeInferer

from collections import defaultdict
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict





def csv_to_ddl(infiles,table_name,map_fields=True,logger=None,encoding=None):

    def log(v):
        if logger:
            logger.debug(v)

    fieldmap = OrderedDict()
    tracker = defaultdict(TypeInferer)
    rows = 0

    for infile in infiles:

        reader = fancycsv.FancyDictReader(infile,no_tabs=True,encoding=encoding)

        if len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise Exception('Column names not unique. Use csv_prepare.py')
        if map_fields:
            mapped_fields = [fancystring.slugify(h) for h in reader.fieldnames]
            fieldmap.update(OrderedDict(zip(reader.fieldnames,mapped_fields)))

        #Now do all the observing
        log("Observing {0} for type info".format(infile.name))
        log("Input encoding is {0}, should be {1}".format(reader.reader.f.encoding,encoding))
        for row in reader:
            rows += 1
            for k,v in row.iteritems():
                tracker[k].observe(v)

    #Data collected now make some psql
    fields_as_sql = ''

    assert len(tracker) > 0

    #Dump info in original order
    for column_name in reader.fieldnames:
        log("Tracker is exporting column {0},{1},{2}".format(column_name,tracker[column_name].count,rows))

        if tracker[column_name].count != rows:
            log("Column was not found in all files!")

        inferer = tracker[column_name]

        if map_fields:
            column_name = fieldmap[column_name]

        fields_as_sql +='\t{0} {1}, \n'.format(column_name,inferer.export())
        
    fields_as_sql = fields_as_sql.strip().strip(',')

    template = '''create table {table_name} ( 
        {fields_as_sql}
    );'''

    ddl = template.format(**dict(table_name=table_name,fields_as_sql=fields_as_sql))
    if map_fields:
        return ddl,fieldmap
    else:
        return ddl


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description='Transforms csv data to a postgres style ddl statement',
            parents=[schema_parser,multifile_input_parser,ddl_file_output_parser]
        )

    args = parser.parse_args()
    print args

    # Establish io vectors
    if args.infile:
        assert os.path.exists(args.infile)
        infile = open(args.infile)
    else:
        infile = sys.stdin
    if args.sqlfile:
        assert os.path.exists(args.sqlfile)
        outfile = open(args.outfile,'wb')
    else:
        outfile = sys.stdout
    #Need schema import to remit found metadata, there is probably a better way to structure this
    with eyeoh.multifileinput(args) as infiles:
        ddl = csv_to_ddl(infiles,args.table_name,map_fields=False)
        outfile.write(ddl)
    




    #todo:
    # enum discovery
    # date observer
    # indxes / fk's
