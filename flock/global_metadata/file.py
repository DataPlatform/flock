import json
import os
from collections import OrderedDict,defaultdict

class Metadata(object):

    def __init__(self):
        # set up file based metadata

        if not self.args.metadatafile:
            self.metadatafile_path = os.path.join(
                self.schema_dir, '.metadata.json')
        else:
            self.metadatafile_path = os.path.abspath(args.metadatafile)
        if os.path.exists(self.metadatafile_path):
            try:
                data = json.loads(open(self.metadatafile_path)
                                  .read(), object_hook=OrderedDict)
            except Exception as e:
                print e
                raise Exception(
                    'Couldn\'t read metadata in file {0}'.format(self.metadatafile_path))
            self.metadata = defaultdict(dict, data)
        else:
            self.metadata = defaultdict(dict)

    def export_metadata(self):
        """
            Only call after successful operations
        """
        self.logger.info('Exporting metadata to' + self.metadatafile_path)
        open(self.metadatafile_path, 'w').write(
            json.dumps(self.metadata, indent=2))

    def set_metadata(self, key, value):
        assert json.dumps(value)
        self.metadata[key] = value

    def get_metadata(self, key, value):
        return self.metadata[value]

    def metadata_keys(self):
        return self.metadata.iterkeys()


    def get_mapper(self, md_key):
        "Returns a function that maps unstandardized fieldnames to standardized ones"

        print 'Attempting to find metadata for key:', md_key
        if md_key in self.metadata:
            # A mapping is configured for this key
            fieldmap = self.metadata[md_key]
            assert type(fieldmap) == OrderedDict
            mapper = lambda x: fieldmap[x]
            # self.logger.debug("*Using the following field mappings*")
            # for k,v in fieldmap.iteritems():
            #     self.logger.debug('{0}: {1}'.format(k,v))
        else:
            mapper = lambda x: x
        return mapper




    # Todo: find a better way of persisting!

    def __exit__(self, type, value, traceback):
        self.export_metadata()

    def __enter__(self):
        return self        