
import re

def slugify(str):
    str = str.strip()
    return re.sub(r'\W+','_',str)
