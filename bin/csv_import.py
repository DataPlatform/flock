#!/usr/bin/python

import sys,argparse,os,csv,json,csv
from flock import db,eyeoh,fancycsv
from flock.parsers import db_parser,external_schema_parser,multifile_input_parser
import argparse




def csv_import(infiles,table,connection,mapper=lambda x: x):
    for infile in infiles:
        cursor = connection.cursor()
        reader = fancycsv.FancyDictReader(infile,no_tabs=True)
        csv_fieldnames = list(reader.fieldnames)
        database_fieldnames = [mapper(x) for x in csv_fieldnames]
        # sys.stderr.write('\n\nCsv names: {0}\nDatabase names: {1}\n\n'.format(csv_fieldnames,database_fieldnames))
        
        # #infile.next() has been called once at this point
        # cursor.copy_from(infile, table, sep=reader.dialect.delimiter, columns=database_fieldnames)

        # Intentional sql injection goes here
        csv_fieldname_formats = ["%({0})s".format(x) for x in csv_fieldnames]
        csv_fieldname_formats = ', '.join(csv_fieldname_formats)
        template = "INSERT INTO {table}({names}) VALUES ({formats})".format(
            table=table,
            names=','.join(database_fieldnames),
            formats=csv_fieldname_formats   
        )
        # print template

        # Use parameter insertion in psycopg for data external to this script!
        cursor.executemany(template, reader)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Transforms csv data to a postgres style insert statement and executes it on the specified database',
            parents=[db_parser,external_schema_parser,multifile_input_parser]
    )
    args = parser.parse_args()
    print args
    connection = db.dial(args.db_uri)
    with eyeoh.multifileinput(args) as infiles:
        csv_import(infiles,args.table,connection)
        connection.commit()
