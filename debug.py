#!/usr/bin/env python

from client import Client

x = Client (8882)
print x.make_query('print')
x.close_connection()
