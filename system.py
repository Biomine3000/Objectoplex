#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import logging
import json
import re

from email.utils import make_msgid

logger = logging.getLogger("system")

def read_until_nul(socket):
    ret = []
    while True:
        c = socket.recv(1)
        if len(c) == 0 or c == '\x00':
            break
        ret.append(c)

    s = ''.join(ret)
    s = s.decode('utf-8')
    return s


class ObjectType(object):
    def __init__(self, content_type, subtype, charset):
        self.content_type = content_type
        self.subtype = subtype
        self.charset = charset

    @classmethod
    def from_string(cls, s):
        m = re.match(ur"(?P<type>\w+)/(?P<subtype>\w+)(;\s+charset=)?(?P<charset>[-\w\d]+)", s)
        content_type = m.group('type')
        subtype = m.group('subtype')
        charset = m.group('charset')
        return ObjectType(content_type, subtype, charset)

    def __unicode__(self, ):
        ret = u'{0}/{1}'.format(self.content_type, self.subtype)
        if self.charset:
            ret += '; charset=' + self.charset
        return ret

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')


class BusinessObject(object):
    def __init__(self, metadata_dict, payload):
        self.properties = metadata_dict
        if 'id' in metadata_dict:
            self.id = metadata_dict['id']
        else:
            self.id = make_msgid()

        self.payload = payload

        if 'size' in metadata_dict:
            self.payload_size = metadata_dict['size']
        else:
            self.payload_size = 0

        self.content_type = ObjectType.from_string(metadata_dict['type'])

    def __unicode__(self, ):
        if self.content_type.content_type == 'text':
            charset = self.content_type.charset
            if not charset:
                charset = 'UTF-8'
            return u'<{0} {1} "{2}">'.format(self.__class__.__name__, self.content_type,
                                             self.payload.decode(charset).encode('ASCII',
                                                                                 'backslashreplace'))
        else:
            return u'<{0} {1}>'.format(self.__class__.__name__, self.content_type)

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def serialize(self):
        self.properties['id'] = self.id
        self.properties['payload_size'] = self.payload_size
        self.properties['type'] = str(self.content_type)
        s = json.dumps(self.properties).encode('utf-8')
        s += '\x00'
        if self.payload_size > 0:
            s += self.payload
        return s

    @classmethod
    def read_from_socket(cls, socket):
        metadata = read_until_nul(socket)
        try:
            metadata_dict = json.loads(metadata)
        except ValueError, ve:
            logger.warning("Couldn't load JSON from '%s'" % metadata)
            return None

        if 'size' in metadata_dict and metadata_dict['size'] > 0:
            payload = socket.recv(metadata_dict['size'])
        else:
            payload = None

        return BusinessObject(metadata_dict, payload)
