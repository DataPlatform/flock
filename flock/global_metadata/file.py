import json
import os
from collections import OrderedDict, defaultdict
from flock.annotate import operation


class Metadata(object):
    """
        Flock component that implements a file based metadata store.
        (This is a separate solution from he database journal in the Driver component)
    """

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

    @operation
    def export_metadata(self):
        """
            Only call after successful operations
        """
        self.logger.info('Exporting metadata to' + self.metadatafile_path)
        open(self.metadatafile_path, 'w').write(
            json.dumps(self.metadata, indent=2))

    @operation
    def set_metadata(self, key, value):
        assert json.dumps(value)
        self.metadata[key] = value

    @operation
    def get_metadata(self, key, value):
        return self.metadata[value]

    @operation
    def metadata_keys(self):
        return self.metadata.iterkeys()


    # Todo: find a better way of persisting!

    def __exit__(self, type, value, traceback):
        self.export_metadata()

    def __enter__(self):
        return self        