import os

# Some initializing

SCHEMA_ROOT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
SCHEMA_NAME = os.path.basename(SCHEMA_ROOT_DIRECTORY)
# *Required Settings* 

#Set these environment variables in flock/schemas/fdps/private 
# and `source flock/init.sh` to pull them into your environment

DATABASES = {
	'dev':os.environ['{{ SCHEMA_NAME }}_dev_db_uri'],
	'test':os.environ['{{ SCHEMA_NAME }}_test_db_uri'],
	'prod':os.environ['{{ SCHEMA_NAME }}_prod_db_uri']
}

DEFAULT_DATABASE = 'dev'

# Data structure for holding permissions that should be applied to all tables
#  - Given in the form dict(permission=['user2','user2']) where permission is something that may GRANTed on a table
#  - See: http://www.postgresql.org/docs/9.2/static/sql-grant.html
#  - More granular permission strategies should be maintained manually

DATABASE_PERMISSIONS = { 
	'all':[os.environ['USER']]
}

#Mainly for granting schema usage: Take set users from permissions above.
DATABASE_USERS = set()
for users in DATABASE_PERMISSIONS.itervalues():
	for user in users:
		DATABASE_USERS.add(user)


LOG_DIRECTORY = os.path.join(os.environ['FLOCK_LOG_DIR'],SCHEMA_NAME)
DATA_DIRECTORY = os.path.join(os.environ['FLOCK_DATA_DIR'],SCHEMA_NAME)


# *Custom settings* 



LOG_TO_EMAIL = 'FLOCK_SMTP_SERVER' in os.environ

if LOG_TO_EMAIL:

	OWNER_EMAIL = os.environ['FLOCK_EMAIL']
	SMTP_SERVER = os.environ['FLOCK_SMTP_SERVER']
	LOG_DIST_LIST = [OWNER_EMAIL]


