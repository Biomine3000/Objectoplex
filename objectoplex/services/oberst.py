# -*- coding: utf-8 -*-
import urllib2
from codecs import getreader

from objectoplex import BusinessObject
from objectoplex.services import Service, timeout


class Oberst(Service):
    __service__ = 'oberst'

    def handle(self, obj):
        self.logger.debug(u"Request {0}".format(obj.metadata))

        try:
            with timeout(5):
                out = urllib2.urlopen('http://biomine.cs.helsinki.fi/oberstdorf/?plain=true')
                reader = getreader('utf-8')(out)
                title = reader.read()

            metadata = {'event': 'services/reply',
                        'in-reply-to': obj.id,
                        'size': len(title),
                        'type': 'text/plain; charset=utf-8'}

            if 'route' in obj.metadata:
                metadata['to'] = obj.metadata['route'][0]

            reply = BusinessObject(metadata, bytearray(title, encoding='utf-8'))
            self.logger.debug(u"Reply {0}".format(reply.metadata))
            return reply
        except Exception, e:
            self.logger.error(u"{0}".format(e))

service = Oberst
