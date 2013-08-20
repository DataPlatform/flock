import sys,argparse,os,csv,json
from flock import fancycsv,eyeoh
from flock.parsers import *



#do work here


def prepare_csv(infile,outfile):
    reader = fancycsv.FancyReader(infile,no_tabs=True)
    writer = fancycsv.UnicodeWriter(outfile)
    header = fancycsv.fix_header(reader.next())
    writer.writerow(header)
    for row in reader:
        writer.writerow(row)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Transforms csv data to a postgres style ddl statement',
        parents=[multifile_input_parser,file_output_parser]
    )
    args = parser.parse_args()

    if args.outfile:
        assert not args.inline

    with eyeoh.multifileinput(args) as infiles:
        for infile in infiles:
            # refix output vectors
            if args.inline:
                args.outfile = infile.name
            #and go
            with eyeoh.fileoutput(args) as outfile:
                prepare_csv(infile,outfile)
                infile.close()







