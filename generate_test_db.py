#!/usr/bin/env python
# encoding: utf-8
from os import environ as env
from datetime import timedelta
from datetime import datetime
from random import randrange, randint, choice
from random import random as randp

from model import *

sqlhub.processConnection = connectionForURI('sqlite:www.sqlite')
Usage.createTable()

yesterday = datetime.now() - timedelta(days=1)


servers = ('server1', 'server2', 'server3')

for i in xrange(randint(100, 1000)):
    domain = 'domain_%s.com' % i
    s = set(choice(servers))
    if randp() > 0.7:
        s.add(choice(servers))

    for server in s:
        bytes = randrange(0, 1000000000)
        hits = randrange(0, 100000)

        Usage(server=server, domain=domain, date=yesterday,
              bytes=bytes, hits=hits)
