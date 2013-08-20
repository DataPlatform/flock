"Collection of code for dealing with csv gotchas and common operations"

#Originally taken from http://docs.python.org/2/library/csv.html but has been modified

import csv, codecs, cStringIO
from collections import defaultdict
from chardet.universaldetector import UniversalDetector

class UTF8Recoder:
    """
        Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding=None):
        if not encoding:
                self.chardet_detector = UniversalDetector()
                tempf = open(f.name,'rb')
                self.encoding = self.charset_detect(tempf)['encoding']
                tempf.close()
        else:
            self.encoding = encoding

        self.reader = codecs.getreader(self.encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

    def charset_detect(self,f, chunk_size=4096):
        self.chardet_detector.reset()
        while 1:
            chunk = f.read(chunk_size)
            if not chunk: break
            self.chardet_detector.feed(chunk)
            if self.chardet_detector.done: break
        self.chardet_detector.close()
        return self.chardet_detector.result

class FancyReader:
    """
        Reader that detects encoding and is a place for some 
        domain specific logic that is unpleasant
    """

    def __init__(self, f, dialect=None, encoding=None, no_tabs =False, **kwds):
        filename = f.name
        f = UTF8Recoder(f,encoding=encoding)
        if not dialect:
            dialect = csv.Sniffer().sniff(open(filename,'rb').read(1024))
        else:
            dialect = csv.excel
        self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.dialect = dialect
        self.no_tabs = no_tabs
        self.header = None

    def fixed_header(self):
        "This rewrites the header to autonumber duplicated fieldnames"
        if not self.header:
            self.next()
        return fix_header(self.header)

    def next(self):
        row = self.reader.next()
        if self.no_tabs:
            row = [' '.join(s.split()) for s in row]

        #Assuming csv files have headers!
        if not self.header:
            self.header = row
        return row

    def __iter__(self):
        return self


class FancyDictReader:
    """
    A UnicodeDictReader and a place for some domain specific logic that is unpleasant. 
    """

    def __init__(self, f, mapper=lambda x: x, unicode_errors="",**kwds):
        # f = UTF8Recoder(f, encoding)
        self.reader = FancyReader(f, **kwds)
        self.fieldnames = [mapper(h) for h in self.reader.fixed_header()]
        self.dialect = self.reader.dialect
        
        #refusing to deal with duplicated fieldnames
        assert len(self.fieldnames) == len(set(self.fieldnames))


    def next(self):
        row = self.reader.next()
        return dict(zip(self.fieldnames,row))

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)



#Ensures field names are unique
def fix_header(header):
    """
        Files come in with duped header names that need to be autonumbered. Must be idempotent.
    """
    seen = defaultdict(list)
    for i,h in enumerate(header):
        seen[h].append(i)
    for k,v in seen.iteritems():
        #If there are duped fieldnames autonumber a suffix after the first one
        if len(v) > 1:
            for j,i in list(enumerate(v))[1:]:
                suffix = ' ' + str(j)
                header[i] += suffix
    return header
