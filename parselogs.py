#!/usr/bin/env python
# encoding: utf-8
import sys
import os
import os.path
from Parser import Parser
from model import Usage
from sqlobject import connectionForURI, sqlhub
from socket import gethostname
import datetime
import logging
logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('parselogs')

LOGDIR = '/var/log/apache2/vhosts'
SERVER = gethostname()
DB = 'sqlite:' + os.path.join(os.getcwd(), 'www.sqlite')

def main():
    sqlhub.processConnection = connectionForURI(DB)
    for domain in os.listdir(LOGDIR):
        logger.info("Parsing logs for domain " + domain)
        logfile = os.path.join(LOGDIR, domain, 'access.log')
        p = Parser()
        p.parse_file(logfile)
        p.parse_file(logfile+'.1')
        for date, d in p.valid_data().items():
            bytes, hits = d
            date_d = datetime.date(*[int(x) for x in date.split("-")])
            try:
                u = Usage.selectBy(date=date_d, domain=domain, server=SERVER)[0]
		continue
            except:
                Usage(server=SERVER, domain=domain, date=date,
                    bytes=bytes, hits=hits)
                logger.debug("Added %s for domain %s" % (date, domain))

if __name__ == '__main__':
    main()

