
import sys
sys.path.append('../flock')
from ddl import TypeInferer

data = (
    #(type,vals)
    ('boolean',["yes", "true", "t", "1", "no", "false", "f", "0"]),
    ('integer',['1','2','3']),
    ('boolean',['0','1']),
    ('float',['0','1.3']),
    ('text',['lol','0','1.3']),
    ('timestamp',['2013-04-29T14:05:29Z','2012-05-01T18:43:39Z']),
    ('timestamp',['2013-04-29T14:05:29Z ']),
    ('timestamp',['2011-08-16 00:11:37'])

)

for answer,values in data:
    ti = TypeInferer()
    for v in values:
        ti.observe(v)
    print 'Passed' if answer == ti.export() else 'Failed' ,(answer,ti.export())
    assert answer == ti.export()
