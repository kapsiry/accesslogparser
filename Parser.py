#!/usr/bin/env python
# encoding: utf-8

import sys
import os
import sys
import re
import logging
import gzip

from datetime import timedelta
from datetime import datetime

from bz2 import BZ2File

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger("Parser")

# 123.123.123.1 - - [15/Nov/2009:06:50:06 +0200] "GET / HTTP/1.1" 404 995 "http://www.kapsi.fi/" "USER AGENT"
# XXXXX tämä matchataan XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX --------ei väliä-------------------
LOG_ENTRY_PATTERN = re.compile('^(?P<ip>[^ ]+) [^ ]+ [^ ]+ \[(?P<date>[^\]]+)\] ".+(?<!\\\)" (?P<status>\d+) (?P<size>\d+|-)')

MONTH_NUMBERS = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05',
          'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09',
          'Oct': '10', 'Nov': '11', 'Dec': '12'}


def parse_line(line):
    result = re.match(LOG_ENTRY_PATTERN, line)
    ip = result.group('ip')
    time = result.group('date')
    status = result.group('status')
    size = result.group('size')
    return { 'ip': ip, 'date': time[0:11], 'status': status, 'size': size}


def date_convert(date):
    try:
        logger.debug("date_convert: " + date)
        day, month, year = date.split("/")
        return "%s-%s-%s" % (year, MONTH_NUMBERS[month], day)
    except:
        return "0000-00-00"


class Parser(object):
    '''
    Class for parsing Apache access log
    
    Parses files line by line and keeps track of the size and number of all
    requests for each day.

    Takes limit_to_date as YYYY-MM-DD
    '''
    def __init__(self, limit_to_date=None):
        self.date = {}
        self.limit_to_date = limit_to_date
        self.lines_read = 0

    def parse(self, line, line_num):
        try:
            parsed = parse_line(line)
        except Exception, e:
            logger.warn(unicode(e))
            logger.warn("Cannot parse line %i: %s" % (line_num, line.replace('\n', '')))
            return

        date = date_convert(parsed['date'])

        if self.limit_to_date and self.limit_to_date != date:
            self.lines_read += 1
            if (self.lines_read % 10000) == 0:
                logger.info("%i lines read" % self.lines_read)
            return

        size = parsed['size']

        if not self.date.has_key(date):
            self.date[date] = [0, 0] # bandwidth, hits
        if not size == '-':
            self.date[date][0] += int(size)
            self.date[date][1] += 1

        self.lines_read += 1

        if (self.lines_read % 10000) == 0:
            logger.info("%i lines read" % self.lines_read)

    def parse_file(self, filename):
        f = None
        try:
            # Open (possibly compressed) log file
            file_size = float(os.path.getsize(filename))
            logger.debug(filename)
            if filename.endswith('.gz'):
                f = gzip.open(filename, 'r')
            elif filename.endswith('.bz2'):
                f = BZ2File(filename, 'r')
            else:
                f = open(filename, 'r')

            read_bytes = 0.0
            last_percentage_reported = 0

            line_num = 0
            for line in f:
                self.parse(line, line_num)
                line_num += 1
                read_bytes += float(len(line))
                percentage = int((read_bytes / file_size) * 100)
                if percentage > last_percentage_reported:
                    logger.info("%i%% of %s read" % (percentage, filename))
                    last_percentage_reported = percentage
        except Exception, e:
            logger.warn(unicode(e))
        finally:
            if f:
                f.close()

    def valid_data(self):
        '''Discard values for the first and last date'''
        dates = sorted(self.date.keys(), key=lambda x: x[0])
        try:
            del dates[0]
            del dates[-1]
        except:
            return {}

        ret = {}
        for date in dates:
            ret[date] = self.date[date]
        return ret


def main():
    from optparse import OptionParser

    parser = OptionParser()

    parser.add_option("-y", dest="yesterday",
                      help="only include yesterday's log entries",
                      action="store_true", default=False)

    opts, args = parser.parse_args()
    
    filelist = args
    print >> sys.stderr, "# Parsing files: %s" % ', '.join(filelist)

    if opts.yesterday:
        yesterday = datetime.now() - timedelta(days=1)
        p = Parser(limit_to_date="%i-%i-%i" % (yesterday.year, yesterday.month, yesterday.day))
    else:
        p = Parser()

    for f in filelist:
        p.parse_file(f)

    for key, value in sorted(p.date.items()):
        print "%s %8.0f MiB %8d reqs" % (key, value[0]/1024/1024, value[1])


if __name__ == '__main__':
    main()
