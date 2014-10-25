import os
import logging
import yaml


class HealthCheckException(Exception):
    pass


def run_these_tests(filename, conn, block=True):
    # logging initialization
    test_collection_name = os.path.basename(filename).split('.')[0]
    logger = logging.getLogger(__name__ + test_collection_name)

    # data structures
    errors = list()
    test_collection = yaml.load(open(filename).read())

    for test in test_collection:

        # get query
        if 'query' in test:
            the_query = test['query']
        else:
            if not os.path.exists(test['queryfile']):
                # path is relative to yaml file
                curdir = os.path.dirname(filename)
                path = os.path.join(curdir, test['queryfile'])
            else:
                # path is absolute
                path = test['queryfile']
            the_query = open(path).read()

        # run query && get result
        curs = conn.execute(the_query)
        # tests should be SQL that return one value!!!!
        query_result = curs.fetchone()[0]

        # log test result
        passed = query_result == test['expected']

        test.update({
            'passed': passed,
            'query_result': query_result
        })

        logger.info('Test "{title}" passed?: {passed} Expected {expected} Found {query_result}'.format(**test))
        print 'Test "{title}" passed?: {passed} Expected {expected} Found {query_result}'.format(**test)

        if not passed:
            errors.append(test)

    # If tests all pass we are done
    if not errors:
        return True
    else:
        # record errors
        for record in errors:
            logger.error('Failed Test "{title}" '.format(**test))

        if block:
            # block indicate that the user would like to block execution on all tests passing
            raise HealthCheckException(errors)



