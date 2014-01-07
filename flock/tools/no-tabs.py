#!/usr/bin/python

import sys

for line in sys.stdin:
    try:
        sys.stdout.write(' '.join(line.split()) + '\n')
    except IOError:
        exit()
