#!/usr/bin/python
"""
    Concatenate csv files in a safe way
"""
import sys,argparse,os,csv,json
from flock.parsers import *
from flock import eyeoh,fancycsv
from collections import defaultdict







def csv_cat(infiles,outfile,append_filename=False):
    allowed_fields = None
    writer = fancycsv.UnicodeWriter(outfile)
    for i,infile in enumerate(infiles):
        filename = os.path.split(infile.name)[1]
        reader = fancycsv.FancyDictReader(infile,no_tabs=True)

        fieldnames = reader.fieldnames[:]
        if append_filename:
            fieldnames.append("_filename")        
        
        if allowed_fields == None:
            allowed_fields = fieldnames
            writer.writerow(allowed_fields)

        else:
            assert sorted(allowed_fields) == sorted(fieldnames)

        for row in reader:
            data = [row[k] for k in allowed_fields]
            data.append(filename)
            writer.writerow(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Concatenate csv files smartly',
            parents=[multifile_input_parser,file_output_parser]
        )
    parser.add_argument('--append_filename', action='store_true')

    args = parser.parse_args()
    with eyeoh.multifileinput(args) as infiles:
        with eyeoh.fileoutput(args) as outfile:
            csv_cat(infiles,outfile,append_filename=args.append_filename)


            



