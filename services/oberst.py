# -*- coding: utf-8 -*-
import urllib2
from codecs import getreader

from system import BusinessObject
from service import Service


class Oberst(Service):
    __service__ = 'oberst'

    def handle(self, obj):
        self.logger.debug(u"Request {0}".format(obj.metadata))

        try:
            out = urllib2.urlopen('http://biomine.cs.helsinki.fi/oberstdorf/?plain=true')
            reader = getreader('utf-8')(out)
            title = reader.read()


            metadata = {'event': 'services/reply',
                        'in-reply-to': obj.id,
                        'size': len(title),
                        'type': 'text/plain; charset=utf-8'}

            if 'route' in obj.metadata:
                metadata['to'] = obj.metadata['route'][0]

            return BusinessObject(metadata, bytearray(title, encoding='utf-8'))
        except Exception, e:
            logger.error(u"{0}".format(e))

service = Oberst
