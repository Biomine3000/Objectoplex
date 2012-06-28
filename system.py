#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import logging
import json
import re
import io

from os.path import getsize
from mimetypes import guess_type
from email.utils import make_msgid

logger = logging.getLogger("system")

def p8(*args, **kwargs):
    from sys import stdout
    from codecs import getwriter
    u8 = getwriter('utf-8')(stdout)
    if 'file' not in kwargs:
        kwargs['file'] = u8
    return print(*args, **kwargs)

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
        m = re.match(ur"(?P<type>\w+)/(?P<subtype>[\w-]+)(;\s+charset=)?(?P<charset>[-\w\d]+)?", s)
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

    def __unicode__(self):
        if self.content_type.content_type == 'text':
            charset = self.content_type.charset
            if not charset:
                charset = 'UTF-8'
            out_payload = self.payload.decode(charset).encode('ASCII', 'backslashreplace')
            if len(out_payload) > 80:
                out_payload = out_payload[:77] + '...'
            return u'<{0} {1} "{2}">'.format(self.__class__.__name__, self.content_type,
                                             out_payload.replace('\n', ' '))
        else:
            return u'<{0} {1}>'.format(self.__class__.__name__, self.content_type)

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def tofile(self, file):
        self.properties['id'] = self.id
        self.properties['payload_size'] = self.payload_size
        self.properties['type'] = str(self.content_type)

        with io.FileIO(file.fileno(), 'w', closefd=False) as f:
            metadata = json.dumps(self.properties).encode('utf-8')
            writer = io.BufferedWriter(f, buffer_size=len(metadata) + self.payload_size + 1)
            writer.write(metadata)
            writer.write('\x00')
            if self.payload_size > 0:
                writer.write(self.payload)
            writer.flush()

    def serialize(self, file=None):
        if file is not None:
            return self.tofile(file)

        self.properties['id'] = self.id
        self.properties['payload_size'] = self.payload_size
        self.properties['type'] = str(self.content_type)
        ret = bytearray(json.dumps(self.properties).encode('utf-8'), encoding='utf-8')
        ret += '\x00'
        if self.payload_size > 0:
            ret.extend(self.payload)
        return ret

    @classmethod
    def from_string(self, string):
        metadata_dict = {
            'size': len(string),
            'type': "text/plain; charset=UTF-8"
            }
        return BusinessObject(metadata_dict, bytearray(string, encoding='utf-8'))

    @classmethod
    def from_file(self, path):
        type, encoding = guess_type(path)

        if type is None:
            raise UnknownFileTypeError("Don't know the content type of %s" % path)

        metadata_dict = {
            'size': getsize(path),
            'type': type
            }

        with io.FileIO(path, 'r') as f:
            contents = bytearray(f.read())
        return BusinessObject(metadata_dict, contents)

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
