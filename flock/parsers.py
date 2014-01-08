import argparse


db_parser = argparse.ArgumentParser(add_help=False)
db_parser.add_argument("db_uri", help="Database URI", type=str)

optional_db_parser = argparse.ArgumentParser(add_help=False)
optional_db_parser.add_argument(
    '-d', "--db_uri", help="Database URI", type=str)

optional_named_db_parser = argparse.ArgumentParser(add_help=False)
optional_named_db_parser.add_argument(
    '-d', "--db_name", help="Database Name", type=str)


optional_tables_parser = argparse.ArgumentParser(add_help=False)
optional_tables_parser.add_argument(
    '-t', '--tables', nargs='*', help='(Optional) Restrict operations to these tables within the schema')


schema_parser = argparse.ArgumentParser(add_help=False)
schema_parser.add_argument(
    '-m', '--metadatafile', type=str, help='Override path to metadata input file')


external_schema_parser = argparse.ArgumentParser(add_help=False)
external_schema_parser.add_argument(
    '-m', '--metadatafile', type=str, help='Override path to metadata input file')
external_schema_parser.add_argument(
    "schema", help="Path to the schema you wish to target", type=str)
external_schema_parser.add_argument(
    "table", help="Target table name", type=str)


file_input_parser = argparse.ArgumentParser(add_help=False)
file_input_parser.add_argument(
    '-i', '--infile', type=str, help='Path to input file( (default stdin)')

multifile_input_parser = argparse.ArgumentParser(add_help=False)
multifile_input_parser.add_argument(
    'infiles', nargs='+', help='Path to input files (default stdin)')

file_output_parser = argparse.ArgumentParser(add_help=False)
file_output_parser.add_argument(
    '-o', '--outfile', type=str, help='Path to output file (default stdout)')
file_output_parser.add_argument('-l', '--inline', action='store_const',
                                const=True, help='When specified, the outfile is the same as the infile')

ddl_file_output_parser = argparse.ArgumentParser(add_help=False)
ddl_file_output_parser.add_argument(
    '-o', '--sqlfile', type=str, help='Override path sql output file')
# ddl_file_output_parser.add_argument('-m','--metadatafile',type=str,help='Override path to metadata output file')

ddl_file_input_parser = argparse.ArgumentParser(add_help=False)
ddl_file_input_parser.add_argument(
    '-m', '--metadatafile', type=str, help='Override path to metadata input file')

tables_parser = argparse.ArgumentParser(add_help=False)
tables_parser.add_argument(
    'tables', nargs='*', help='(Optional) Restrict operations to these tables within the schema')
