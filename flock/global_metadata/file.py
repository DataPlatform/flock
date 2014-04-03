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