#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import os.path
import datetime
import logging

from socket import gethostname

from sqlobject import connectionForURI, sqlhub

from Parser import Parser
from model import Usage

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('parselogs')

def main(log_dir, server_name, database_spec):
    sqlhub.processConnection = connectionForURI(database_spec)
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    for domain in os.listdir(log_dir):
        logger.info("Parsing logs for domain " + domain)
        logfile = os.path.join(log_dir, domain, 'access.log')
        # p = Parser()
        p = Parser(limit_to_date="%i-%i-%i" % (yesterday.year, yesterday.month, yesterday.day))
        p.parse_file(logfile)
        p.parse_file(logfile+'.1')
        for date, d in p.valid_data().iteritems():
            bytes, hits = d
            date_d = datetime.date(*[int(x) for x in date.split("-")])
            try:
                u = Usage.selectBy(date=date_d, domain=domain, server=server_name)[0]
                continue
            except:
                Usage(server=server_name, domain=domain, date=date,
                      bytes=bytes, hits=hits)
                logger.debug("Added %s for domain %s" % (date, domain))


if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser()

    parser.add_option("-l", "--log-dir", dest="log_dir",
                      help="read logs from DIRECTORY with vhost log dirs", metavar="DIRECTORY",
                      default='/var/log/apache2/vhosts')
    parser.add_option("-s", "--server-name", dest="server_name",
                      help="override the canonical hostname", metavar="SERVER-NAME",
                      default=gethostname())
    parser.add_option("-d", "--database-spec", dest="db_spec",
                      help="database to connect to", metavar="DB-SPEC",
                      default='sqlite:' + os.path.join(os.getcwd(), 'www.sqlite'))

    opts, args = parser.parse_args()
    main(opts.log_dir, opts.server_name, opts.db_spec)

