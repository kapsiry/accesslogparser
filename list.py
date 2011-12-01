#!/usr/bin/env python
# encoding: utf-8
from os import environ as env
from datetime import timedelta
from datetime import datetime
from optparse import OptionParser
import sqlite3

try:
    import psycopg2
    conn = psycopg2.connect(database=env["DATABASE_NAME"], user=env["DATABASE_USER"],
                            password=env["DATABASE_PASSWORD"], host=env["DATABASE_HOST"])
except:
    conn = sqlite3.connect("www.sqlite")

c = conn.cursor()
yesterday = datetime.now() - timedelta(days=1)

parser = OptionParser()
parser.add_option("--hits", "--file", dest="hits",
                  action="store_true", default=False,
                  help="order by hits")
opts, args = parser.parse_args()

if opts.hits:
    if isinstance(conn, sqlite3.Connection):
        q = ("select domain, sum(bytes) as b, sum(hits) as h from usage " +
             "where date=date(?, '0 days') " +
             "group by domain " +
             "order by h desc",
             (yesterday,))
    else:
        q = ("select domain, sum(bytes) as b, sum(hits) as h from usage " +
             "where date=date_trunc('day', %s) " +
             "group by domain " +
             "order by h desc",
             (yesterday,))
else:
    if isinstance(conn, sqlite3.Connection):
        q = ("select domain, sum(bytes) as b, sum(hits) as h from usage " +
             "where date=date(?, '0 days') " +
             "group by domain " +
             "order by b desc",
             (yesterday,))
    else:
        q = ("select domain, sum(bytes) as b, sum(hits) as h from usage " +
             "where date=date_trunc('day', ?) " +
             "group by domain " +
             "order by b desc",
             (yesterday,))

# print c.mogrify(*q)
c.execute(*q)

def format_bytes(b):
    kilobytes = b / 1024
    megabytes = kilobytes / 1024
    gigabytes = megabytes / 1024
    terabytes = gigabytes / 1024

    if terabytes > 10:
        return "%.2d" % terabytes + " TiB"
    elif gigabytes > 10:
        return "%.2d" % gigabytes + " GiB"
    elif megabytes > 10:
        return "%.2d" % megabytes + " MiB"
    elif kilobytes > 1:
        return "%.2d" % kilobytes + " KiB"
    else:
        return "%.2d" % b + " B"

def format_row(t):
    return "%-35s\t%-10s\t%-10s" % (t[0], format_bytes(t[1]), str(t[2]))

try:
    for r in c:
        print format_row(r)
except IOError, ie:
    pass
