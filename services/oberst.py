# -*- coding: utf-8 -*-
import urllib2

from system import BusinessObject
from service import Service


class Oberst(Service):
    __service__ = 'oberst'

    def handle(self, obj):
        title = urllib2.urlopen('http://biomine.cs.helsinki.fi/oberstdorf/?plain=true').read().replace('\n', '')

        return BusinessObject({'event': 'service/reply',
                               'in-reply-to': obj.id,
                               'size': len(title),
                               'type': 'text/plain; charset=utf-8'},
                              bytearray(title, encoding='utf-8'))

service = Oberst
