import os
import sys
import inspect
import logging

# Required Settings

SCHEMA_NAME = os.path.basename(os.path.basename(os.getcwd()))
SCHEMA_ROOT_DIRECTORY = os.path.abspath(os.path.join(os.environ.get('HOME',''),'.flock',SCHEMA_NAME))
LOG_DIRECTORY = os.environ.get('FLOCK_LOG_DIR',os.path.join(SCHEMA_ROOT_DIRECTORY,'log'))
DATA_DIRECTORY = os.environ.get('FLOCK_DATA_DIR',os.path.join(SCHEMA_ROOT_DIRECTORY,'data'))
SCHEMA_DIRECTORY = os.environ.get('FLOCK_SCHEMA_DIR',os.path.join(SCHEMA_ROOT_DIRECTORY,'schema'))


_db_username = os.environ.get('USER','flock')

DATABASES = {
    'test': os.environ.get('FLOCK_TEST_DB_URI','postgres://{0}:@localhost/flock_test'.format(_db_username)),
}

ENVIRONMENT = DEFAULT_DATABASE = 'test'

# Data structure for holding permissions that should be applied to all tables
#  - Given in the form dict(permission=['user2','user2']) where permission is something that may GRANTed on a table
#  - See: http://www.postgresql.org/docs/9.2/static/sql-grant.html
#  - More granular permission strategies should be maintained manually

DATABASE_PERMISSIONS = {
	# example syntax, this is implied
    'all': [_db_username]
}

# Mainly for granting schema usage: Take set users from permissions above.

DATABASE_USERS = set()
for users in DATABASE_PERMISSIONS.itervalues():
    for user in users:
        DATABASE_USERS.add(user)


LOG_TO_EMAIL = 'FLOCK_SMTP_SERVER' in os.environ

if LOG_TO_EMAIL:

    OWNER_EMAIL = os.environ['FLOCK_EMAIL']
    SMTP_SERVER = os.environ['FLOCK_SMTP_SERVER']
    LOG_DIST_LIST = [OWNER_EMAIL]



logger = logging.getLogger(__name__)
for name, obj in inspect.getmembers(sys.modules[__name__]):
    if not name.startswith('_') and name.isupper():
        logger.info('default setting for {} {}'.format(name,obj))