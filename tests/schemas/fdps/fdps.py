#-------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      eric.hamano@cfpb.gov
#
# Created:     16/07/2013
#-------------------------------------------------------------------------

import requests
import urllib2
import hashlib
import os
from lxml import etree as et
from collections import defaultdict
import datetime as dt

# Initialize Proxy

if 'https_proxy' in os.environ:
    proxy_support = urllib2.ProxyHandler({"https": os.environ['https_proxy']})
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)


request_baseurl = 'https://www.fpds.gov/ezsearch/fpdsportal?s=FPDS&indexName=awardfull&templateName=1.4&q=LAST_MOD_DATE%3A{year}%2F{month}%2F{day}+FUNDING_AGENCY_ID%3A%22955F%22&rss=1&feed=atom0.3&start={offset}'


request_date_format = '%Y/%m/%d'

if not os.path.exists('.cache'):
    os.makedirs('.cache')


def walk(node, tag_names=None):
    if not tag_names:
        tag_names = []

    tag_names.append(str(node.tag).split('}').pop().strip())
    value = str(node.text).replace('\r', '').replace('\n', '').strip()

    yield tag_names, None, value

    for attrib, value in node.items():
        yield tag_names, attrib, value

    for child in node.getchildren():
        for row in walk(child, tag_names=tag_names[:]):
            yield row


def get_data(beginning, days, request_increment=10):
    """
        Generator that traverses the FDPS api and flattens found data.
    """

    # Initialize response_xml
    response_xml = ''

    n = 0

    if days:
        days = range(days)
    else:
        days = range(
            len(list(all_the_days_till_yesterday(beginning=beginning))))

    for day in days:

        request_index = 0

        while 1:

            # Create request url string
            request_ModifiedDate = (beginning + dt.timedelta(
                days=day))
            kwargs = dict(
                year=str(request_ModifiedDate.year),
                month=str(request_ModifiedDate.month).zfill(2),
                day=str(request_ModifiedDate.day).zfill(2),
                offset=request_index
            )
            request_url = request_baseurl.format(**kwargs)
            # print request_url

            # Submit request url and save response
            # k = hashlib.sha1(request_url).hexdigest()
            k = request_ModifiedDate.isoformat() + '_4_' + str(request_index)
            local = os.path.join('.cache', k)

            if os.path.exists(local):
                response_xml = open(local, 'rb').read()
            else:
                response_xml = urllib2.urlopen(request_url).read()
                # response_xml = requests.get(request_url,proxies=proxies).content
                open(local, 'wb').write(response_xml)

            root = et.fromstring(response_xml)

            # Loop through entries in response XML
            for i, entry in enumerate(root.findall('{http://www.w3.org/2005/Atom}entry')):

                row = dict()
                for tag_names, attrib, value in walk(entry):

                    # constructing unique column name based on tag name and
                    # parent tag name also need to conform to 64 char limits
                    column = '_'.join(tag_names[-2:])
                    if attrib:
                        column += '_' + attrib
                        # print column,value
                    if len(column) > 63:
                        # print column,
                        column = '_'.join(column.split('_')[1:])
                        # print column
                    if column in row:
                        assert row[column] == value
                    else:
                        row[column] = value
                # print row
                yield row

                n += 1

            if root.findall('{http://www.w3.org/2005/Atom}entry') == []:
                break
            else:
                request_index += request_increment


# Get a list of column_names based on a sample

def get_column_names(data):
    column_names = list()
    counts = defaultdict(int)
    # for each row in the data
    for row in data:
        for column in row.iterkeys():
            if column not in column_names:
                column_names.append(column)
            counts[column] += 1
    return column_names, counts


def get_fdps_data(beginning, days, column_names=None):

    data = list(get_data(beginning, days))

    if column_names == None:
        column_names, counts = get_column_names(data)
    else:
        _, counts = get_column_names(data)
    return data, column_names


def get_fdps_slice(beginning, days, column_names=None):
    data, column_names = get_fdps_data(beginning, days)
    assert len(list(set(column_names))) == len(column_names)

    slice = list([column_names[:]])
    for row in data:
        slice.append([(row[k] if k in row else '') for k in column_names])
    return slice

# def all_the_days_till_yesterday(beginning=dt.date(2010, 1, 1)):
#     today = dt.datetime.now().date()
#     delta = dt.timedelta()
#     while (beginning + delta) < today:

#         yield beginning + delta
#         delta += dt.timedelta(days=1)
