# -*- coding: utf-8 -*-
from time import sleep

from service import Service

class Oberst(Service):
    __service__ = 'oberst'

    def handle(self, obj):
        title = urllib2.urlopen('http://biomine.cs.helsinki.fi/oberstdorf/?plain=true').read().replace('\n', '')

        return BusinessObject({'event': 'service/reply',
                               'in-reply-to': obj.id,
                               'size': len(title),
                               'type': 'text/plain; charset=utf-8'},
                              title)


service = Oberst
