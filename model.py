#!/usr/bin/env python
# encoding: utf-8
from sqlobject import SQLObject, connectionForURI, sqlhub
from sqlobject import StringCol, DecimalCol, DateCol, ForeignKey
from datetime import datetime
    
class Usage(SQLObject):
    server = StringCol()
    domain = StringCol()
    date = DateCol(default=datetime.now())
    bytes = DecimalCol(size=20, precision=0)
    hits = DecimalCol(size=10, precision=0)
    
    def __unicode__(self):
        return '''%(date)s %(domain)s: %(kb).0f kB / %(hits)d requests''' % {
        'date': self.date, 'kb': self.bytes/1000, 'hits': self.hits,
        'domain': self.domain}

def main():
    sqlhub.processConnection = connectionForURI('sqlite:/:memory:')
    SERVER='kirsikka'
    DOMAIN='yx.fi'
    Usage.createTable()
    Usage(server=SERVER, domain=DOMAIN, date=datetime(2009, 10, 11), bytes=2900,
        hits=100)
    Usage(server=SERVER, domain=DOMAIN, date=datetime(2009, 10, 12), bytes=5000,
        hits=250)
    Usage(server=SERVER, domain=DOMAIN, date=datetime(2009, 10, 13), bytes=7900,
        hits=200)
    for i in Usage.select():
        print unicode(i)

if __name__ == '__main__':
    main()
