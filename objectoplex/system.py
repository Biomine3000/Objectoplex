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
from socket import socket as actual_socket
from datetime import datetime, timedelta

logger = logging.getLogger("system")

class InvalidObject(Exception): pass
class CannotConvertToPython(Exception): pass

_MAX_PAYLOAD_BYTES = 2048


def read_until_nul(socket, last_activity_timeout_secs=5, read_timeout_secs=120):
    started = datetime.now()
    last_activity = datetime.now()
    ret = bytearray()
    while len(ret) <= _MAX_PAYLOAD_BYTES:
        now = datetime.now()
        if now - timedelta(seconds=last_activity_timeout_secs) > last_activity or \
           now - timedelta(seconds=read_timeout_secs) > started:
            raise InvalidObject("Timed out reading metadata")

        char = socket.recv(1)
        if len(char) == 0 or char == '\x00':
            break
        ret.extend(char)
        last_activity = now

    return ret


class ObjectType(object):
    def __init__(self, content_type, subtype, metadata=None):
        self.content_type = content_type
        self.subtype = subtype

        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata

    @classmethod
    def from_string(cls, s):
        m = re.match(ur"(?P<type>\w+)/(?P<subtype>[\w-]+)(;\s+charset=)?(?P<charset>[-\w\d]+)?", s)
        content_type = m.group('type')
        subtype = m.group('subtype')
        metadata = {'charset': m.group('charset')}

        return ObjectType(content_type, subtype, metadata=metadata)

    def __unicode__(self):
        ret = u'{0}/{1}'.format(self.content_type, self.subtype)
        if self.metadata.get('charset', None) is not None:
            ret += '; charset=' + self.metadata['charset']
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
        if self.of_content_type('text') and self.payload and not self.event:
            return u'<{0} {1} "{2}">'.format(self.__class__.__name__, self.content_type,
                                             self.text_payload_snippet().replace('\n', ' '))
        elif self.of_content_type('text') and self.payload and self.event:
            return u'<{0} {1}; {2} "{3}">'.format(self.__class__.__name__, self.content_type,
                                                  self.event,
                                                  self.text_payload_snippet().replace('\n', ' '))
        elif self.event:
            return u'<{0} {1}; {2}>'.format(self.__class__.__name__, self.content_type, self.event)
        else:
            return u'<{0} {1}>'.format(self.__class__.__name__, self.content_type)

    def text_payload_snippet(self, max_length=120):
        charset = self.content_type.metadata.get('charset', 'utf-8')
        if charset is None:
            charset = 'utf-8'

        try:
            ret = self.payload.decode(charset).encode('ASCII', 'backslashreplace')
        except Exception, e:
            logger.error(u"{0} while decoding payload with charset {1}".format(e, charset))
            return u''

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

        # If the caller forgets to use named parameter, the first parameter
        # gets bound to file and if it's a socket, we can fix the situation
        # easily here.
        if isinstance(file, actual_socket):
            socket = file
            file = None

        if file is not None:
            return self._to_file(ret, file)
        elif socket is not None:
            return self._to_socket(ret, socket)
        else:
            return ret

    def payload_as_python(self):
        if self.content_type and \
            (self.content_type.subtype == 'json' or
             self.content_type.subtype == 'javascript'):
            payload_string = self.payload.decode('utf-8')
            return json.loads(payload_string)
        else:
            raise CannotConvertToPython("Type %s can't be transformed to Python." %
                                          str(self.content_type))

    @classmethod
    def from_string(self, string):
        metadata_dict = {
            'size': len(string),
            'type': "text/plain; charset=UTF-8"
            }
        return BusinessObject(metadata_dict, bytearray(string, encoding='utf-8'))

    @classmethod
    def from_python(self, metadata, obj):
        payload = json.dumps(obj)
        metadata['size'] = len(payload)
        metadata['type'] = "application/json"
        return BusinessObject(metadata, bytearray(string, encoding='utf-8'))

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
    def read_from_socket(cls, socket, last_activity_timeout_secs=5, read_timeout_secs=120):
        started = datetime.now()
        last_activity = datetime.now()
        metadata = read_until_nul(socket, last_activity_timeout_secs, read_timeout_secs)
        try:
            metadata = metadata.decode('utf-8')
            metadata_dict = json.loads(metadata)

            if 'size' in metadata_dict and metadata_dict['size'] > 0:
                # logger.debug("Reading payload of size %i" % metadata_dict['size'])
                payload = bytearray()
                while len(payload) < metadata_dict['size']:
                    now = datetime.now()
                    if now - timedelta(seconds=last_activity_timeout_secs) > last_activity or \
                       now - timedelta(seconds=read_timeout_secs) > started:
                        raise InvalidObject("Timed out while reading payload from %s" %
                                            str(socket))

                    received = socket.recv(metadata_dict['size'] - len(payload))
                    if len(received) > 0:
                        last_activity = now
                    payload.extend(received)

                assert(len(payload) == metadata_dict['size'])
            else:
                # logger.debug("Not reading payload")
                payload = None

            return BusinessObject(metadata_dict, payload)
        except ValueError, ve:
            if len(metadata) > 100:
                metadata = metadata[0:75] + "..."
            logger.warning("Couldn't load JSON from '%s'" % metadata)
            return None

