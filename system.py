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
    ret = bytearray()
    while True:
        char = socket.recv(1)
        if len(char) == 0 or char == '\x00':
            break
        ret.extend(char)

    return ret


class InvalidObject(Exception): pass


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

    def __unicode__(self):
        ret = u'{0}/{1}'.format(self.content_type, self.subtype)
        if self.charset:
            ret += '; charset=' + self.charset
        return ret

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')


class BusinessObject(object):
    def __init__(self, metadata_dict, payload):
        self.metadata = metadata_dict
        if 'id' in metadata_dict:
            self.id = metadata_dict['id']
        else:
            self.id = make_msgid()

        self.payload = payload

        if 'size' in metadata_dict:
            self.size = metadata_dict['size']
        else:
            self.size = 0

        if 'type' in metadata_dict:
            self.content_type = ObjectType.from_string(metadata_dict['type'])
        else:
            self.content_type = None

        self.event = metadata_dict.get('event', None)

    def of_content_type(self, content_type):
        if self.content_type and \
               self.content_type.content_type == content_type:
            return True
        else:
            return False

    def __unicode__(self):
        if self.of_content_type('text') and not self.event:
            return u'<{0} {1} "{2}">'.format(self.__class__.__name__, self.content_type,
                                             self.text_payload_snippet().replace('\n', ' '))
        elif self.of_content_type('text') and self.event:
            return u'<{0} {1}; {2} "{3}">'.format(self.__class__.__name__, self.content_type,
                                                  self.event,
                                                  self.text_payload_snippet().replace('\n', ' '))
        elif self.event:
            return u'<{0} {1}; {2}>'.format(self.__class__.__name__, self.content_type, self.event)
        else:
            return u'<{0} {1}>'.format(self.__class__.__name__, self.content_type)

    def text_payload_snippet(self, max_length=120):
        charset = self.content_type.charset
        if not charset:
            charset = 'utf-8'
        ret = self.payload.decode(charset).encode('ASCII', 'backslashreplace')
        if len(ret) > max_length:
            ret = ret[:max_length - 3] + '...'
        return ret

    def __str__(self):
        return unicode(self).encode('ASCII', 'backslashreplace')

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def _to_file(self, serialized, file):
        with io.FileIO(file.fileno(), 'w', closefd=False) as f:
            writer = io.BufferedWriter(f, buffer_size=len(serialized))
            writer.write(serialized)
            writer.flush()
            file.flush()

    def _to_socket(self, serialized, socket):
        sent_total = 0
        while sent_total < len(serialized):
            sent = socket.send(serialized[sent_total:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            sent_total += sent
        return len(serialized), sent_total

    def serialize(self, file=None, socket=None):
        """
        Serializes the object to bytearray, file or socket.
        """
        self.metadata['id'] = self.id
        self.metadata['size'] = self.size
        if self.content_type is not None:
            self.metadata['type'] = str(self.content_type)

        ret = bytearray(json.dumps(self.metadata, ensure_ascii=False), encoding='utf-8')
        ret += '\x00'
        if self.size > 0:
            ret.extend(self.payload)

        if file is not None:
            return self._to_file(ret, file)
        elif socket is not None:
            return self._to_socket(ret, socket)
        else:
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
            metadata = metadata.decode('utf-8')
            metadata_dict = json.loads(metadata)

            if 'size' in metadata_dict and metadata_dict['size'] > 0:
                logger.debug("Reading payload of size %i" % metadata_dict['size'])
                payload = bytearray()
                while len(payload) < metadata_dict['size']:
                    payload.extend(socket.recv(metadata_dict['size'] - len(payload)))

                assert(len(payload) == metadata_dict['size'])
            else:
                logger.debug("Not reading payload")
                payload = None

            return BusinessObject(metadata_dict, payload)
        except ValueError, ve:
            logger.warning("Couldn't load JSON from '%s'" % metadata)
            return None

