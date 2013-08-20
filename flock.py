"""
	CLI module for flock: Does some fancy importing and inspection to discover
	"commands" that can be called. (Commands are designated with the @command decorator.
	See schemas/fdpc/schema.py for an example)
"""

import IPython,os,inspect,glob
from flock.parsers import optional_named_db_parser,schema_parser,optional_tables_parser
from flock.schema import get_schema,FlockSchema
from flock.fancyimport import import_from_path
import argparse
from argparse import RawTextHelpFormatter


# create the top-level parser

parser = argparse.ArgumentParser(
	prog='python flock.py',
	description='Set off all operations with this module',
	formatter_class=RawTextHelpFormatter,
	parents = [optional_named_db_parser,schema_parser]
)

subparsers = parser.add_subparsers(
	title='Flock of FlockSchemas (Select one of the following with -h for more specific help)',
	dest='schema',
	metavar='<schema>',
)


#Import schema classes

all_schema_files = glob.glob('schemas/*/schema.py')
all_schemas = dict()


for schema_file_path in all_schema_files:

	schema_path,_ = os.path.split(schema_file_path)
	_,schema_name = os.path.split(schema_path)


	mod = import_from_path(schema_file_path)

	schema_class = getattr(mod,'Schema')

	#Can't have two schemas with the same name
	assert schema_name not in all_schemas

	all_schemas[schema_name] = (schema_path,schema_class)

	schema_parser = subparsers.add_parser(
		schema_name, 
		description="Schema description",
		help=inspect.getdoc(schema_class),
	)

	command_subparsers = schema_parser.add_subparsers(
	title='Commands for this schema',
	dest='command',
	metavar='<command>'
	)



	for attr_name in dir(schema_class):
		attr = getattr(schema_class,attr_name)

		if inspect.ismethod(attr):

			if 'command' in attr.im_func.__dict__:
				schema_parser = command_subparsers.add_parser(
					attr.im_func.func_name, 
					help=attr.im_func.func_doc,
					parents=[optional_tables_parser]
				)

args = parser.parse_args()
the_schema_path,the_schema_class = all_schemas[args.schema]
args.schema = the_schema_path#legacy
with get_schema(args,schema_class=the_schema_class) as schema:
	getattr(schema,args.command)()








