#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import re
import logging as logger
from datetime import datetime
import gzip
from bz2 import BZ2File

# 123.123.123.1 - - [15/Nov/2009:06:50:06 +0200] "GET / HTTP/1.1" 404 995 "http://www.kapsi.fi/" "USER AGENT"
# XXXXX tämä matchataan XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX --------ei väliä-------------------
PATTERN = re.compile('^(?P<ip>[^ ]+) [^ ]+ [^ ]+ \[(?P<date>[^\]]+)\] ".+(?<!\\\)" (?P<status>\d+) (?P<size>\d+|-)')

def parse_line(line):
    result = re.match(PATTERN, line)
    ip = result.group('ip')
    time = result.group('date')
    status = result.group('status')
    size = result.group('size')
    return { 'ip': ip, 'date': time[0:11], 'status': status, 'size': size}

MONTHS = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05',
          'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09',
          'Oct': '10', 'Nov': '11', 'Dec': '12'}
def date_convert(date):
    try:
	logger.debug("date_convert: " + date)
        day, month, year = date.split("/")
        return "%s-%s-%s" % (year, MONTHS[month], day)
    except:
        return "0000-00-00"

class Parser():
    '''Class for parsing Apache access log
    Parses the access log line by line and keeps track of the size and
    number of all requests, for each day.'''
    def __init__(self):
        self.d = {}
    
    def parse(self, line):
        parsed = parse_line(line)
        size = parsed['size']
        date = date_convert(parsed['date'])
        if not self.d.get(date):
            self.d[date] = [0, 0] # bandwidth, hits
        if not size == '-':
            self.d[date][0] += int(size)
            self.d[date][1] += 1

    def parse_file(self, filename):
        f = None
        try:
            # Open (possibly compressed) log file
            logger.debug(filename)
            if filename.endswith('.gz'):
                f = gzip.open(filename, 'r')
            elif filename.endswith('.bz2'):
                f = BZ2File(filename, 'r')
            else:
                f = open(filename, 'r')

            for line in f:
                self.parse(line)
        except Exception, e:
            logger.warn(unicode(e))
        finally:
            if f:
                f.close()


    def valid_data(self):
        '''Discard values for the first and last date'''
        dates = sorted(self.d.keys(), key=lambda x: x[0])
        try:
            del dates[0]
            del dates[-1]
        except:
            return {}
        d = {}
        for a, b in dates:
            d[a] = self.d[b]
        return d
    
def main():
    filelist = sys.argv[1:]
    print filelist
    p = Parser()
    for f in filelist:
        p.parse_file(f)
    for (key, value) in sorted(p.d.items()):
        print "%s %8.0f MiB %8d reqs" % (key, value[0]/1024/1024, value[1])
    
if __name__ == '__main__':
    main()