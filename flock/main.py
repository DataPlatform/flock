"""
	CLI module for flock: Does some fancy importing and inspection to discover
	"commands" that can be called. (Commands are designated with the @command decorator.
	See schemas/fdpc/schema.py for an example)
"""

# import IPython#,os,#inspect#,glob
# from flock.schema import get_schema,FlockSchema
# from flock.fancyimport import import_from_path

from flock.parsers import optional_named_db_parser,schema_parser,optional_tables_parser
import argparse
from argparse import RawTextHelpFormatter

def enter(self):
	# create the top-level parser

	parser = argparse.ArgumentParser(
		prog='python schema.py',
		description='Set off all operations specific to this schema',
		formatter_class=RawTextHelpFormatter,
		parents = [optional_named_db_parser,schema_parser]
	)

	subparsers = parser.add_subparsers(
		title='Title the first',
		dest='schema',
		metavar='<schema>',
	)


	#Iterate through every attribute on the schema class and register commands
	for attr_name in dir(schema_class):
		attr = getattr(schema_class,attr_name)
		#Only callables (methods in this context) can have decorators
		if inspect.ismethod(attr):

			if 'command' in attr.im_func.__dict__:
				# schema_parser = command_subparsers.add_parser(
				subparsers.add_parser(
					attr.im_func.func_name, 
					help=attr.im_func.func_doc,
					parents=[optional_tables_parser]
					)
	#Parse the args
	args = parser.parse_args()
	#Retrieve and call the command
	getattr(self,args.command)()



if __name__ == '__main__':
    main()




