#!/usr/bin/env python
# encoding: utf-8
from os import environ as env
from datetime import timedelta
from datetime import datetime
from optparse import OptionParser
import sqlite3


def get_query(connection, hits=False):
    if hits:
        if isinstance(connection, sqlite3.Connection):
            return "select domain, sum(bytes) as b, sum(hits) as h from usage " + \
                   "where date=date(?, '0 days') " + \
                   "group by domain " + \
                   "order by h desc"
        else:
            return "select domain, sum(bytes) as b, sum(hits) as h from usage " + \
                   "where date=date_trunc('day', %s) " + \
                   "group by domain " + \
                   "order by h desc"

    else:
        if isinstance(connection, sqlite3.Connection):
            return "select domain, sum(bytes) as b, sum(hits) as h from usage " + \
                   "where date=date(?, '0 days') " + \
                   "group by domain " + \
                   "order by b desc"
        else:
            return "select domain, sum(bytes) as b, sum(hits) as h from usage " + \
                   "where date=date_trunc('day', %s) " + \
                   "group by domain " + \
                   "order by b desc"


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


if __name__ == '__main__':
    try:
        import psycopg2
        conn = psycopg2.connect(database=env["DATABASE_NAME"], user=env["DATABASE_USER"],
                                password=env["DATABASE_PASSWORD"], host=env["DATABASE_HOST"])
    except:
        conn = sqlite3.connect("www.sqlite")

    c = conn.cursor()

    parser = OptionParser()
    parser.add_option("--hits", "--file", dest="hits",
                      action="store_true", default=False,
                      help="order by hits")
    parser.add_option("--days-ago", "-d", dest="days_ago",
                      type="int", default=1,
                      help="1 for yesterday, 2 for day before that etc")
    opts, args = parser.parse_args()

    day = datetime.now() - timedelta(days=opts.days_ago)

    # print c.mogrify(*q)
    c.execute(get_query(conn, hits=opts.hits), (day,))

    try:
        for r in c:
            print format_row(r)
    except IOError, ie:
        pass
