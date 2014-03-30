import os

# Some initializing

SCHEMA_ROOT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
SCHEMA_NAME = os.path.basename(SCHEMA_ROOT_DIRECTORY)


_username = os.environ.get('USER','flock')

DATABASES = {
    'test': os.environ.get('FLOCK_TEST_DB_URI','postgres://{0}:password@localhost/flock_test'.format(_username)),
}

#Leaving DEFAULT_DATABASE for now so as not to break things. ENVIRONMENT is semantically better
ENVIRONMENT = DEFAULT_DATABASE = 'test'

DATABASE_PERMISSIONS = {
    'all': [_username]
}

# Mainly for granting schema usage: Take set users from permissions above.
DATABASE_USERS = set()
for users in DATABASE_PERMISSIONS.itervalues():
    for user in users:
        DATABASE_USERS.add(user)


LOG_DIRECTORY = os.path.join(os.environ.get('FLOCK_LOG_DIR','.log'), SCHEMA_NAME)
DATA_DIRECTORY = os.path.join(os.environ.get('FLOCK_DATA_DIR','.data'), SCHEMA_NAME)


# *Custom settings*

# This dataset has only one table
TABLES = ['procurements']


LOG_TO_EMAIL = 'FLOCK_SMTP_SERVER' in os.environ

if LOG_TO_EMAIL:

    OWNER_EMAIL = os.environ['FLOCK_EMAIL']
    SMTP_SERVER = os.environ['FLOCK_SMTP_SERVER']
    LOG_DIST_LIST = [OWNER_EMAIL]
