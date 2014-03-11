# -*- coding: utf-8 -*-
from __future__ import print_function

import select
import json

from sys import stdout
from datetime import datetime, timedelta

from system import BusinessObject


non_readable_keys = frozenset(['route', 'id', 'in-reply-to', 'avoid', 'size',
                               'event', 'type', 'to', 'routing-id', 'sha1'])
system_parsed_keys = frozenset(['type', 'event'])


def _total_seconds(delta):
    return (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1e6) / 1e6

def reply_for_object(obj, sock, timeout_secs=1.0, select=select):
    """
    Waits for a reply to a sent object (connected by in-reply-to field).

    Returns the object and seconds elapsed as tuple (obj, secs).

    select-module is parameterizable (if not given, Python standard one is used);
    useful for e.g. gevent select.
    """
    started = datetime.now()
    delta = timedelta(seconds=timeout_secs)
    while True:
        rlist, wlist, xlist = select.select([sock], [], [], 0.0001)

        if datetime.now() > started + delta:
            return None, timeout_secs

        if len(rlist) == 0:
            continue

        reply = BusinessObject.read_from_socket(sock)

        if reply is None:
            raise InvalidObject
        elif reply.metadata.get('in-reply-to', None) == obj.id:
            took = datetime.now() - started
            if hasattr(took, 'total_seconds'):
                return reply, took.total_seconds()
            else:
                return reply, _total_seconds(took)


def read_object_with_timeout(sock, timeout_secs=1.0, select=select):
    rlist, wlist, xlist = select.select([sock], [], [], timeout_secs)

    if len(rlist) > 0:
        return BusinessObject.read_from_socket(sock)

def registration_object(client_name, user_name):
    metadata = {
        'event': 'services/request',
        'name': 'clients',
        'request': 'join',
        'client': client_name,
        'user': user_name
        }
    return BusinessObject(metadata, None)

def subscription_object():
    metadata = {
        'event': 'routing/subscribe',
        'receive-mode': 'all',
        'types': 'all'
        }
    return BusinessObject(metadata, None)

def format_readably(obj, file=None, no_payload=False, include=set(), exclude=set()):
    global non_readable_keys
    global system_parsed_keys
    snippets = []
    hidden_keys = set()

    text = None
    if no_payload is False and \
           obj.of_content_type('text') and \
           'size' in obj.metadata and \
           obj.metadata['size'] > 0:
        charset = obj.content_type.metadata['charset']
        if charset is None:
            text = unicode(obj.payload.decode('utf-8'))
        else:
            text = unicode(obj.payload.decode(charset))

    print_keys = set()

    for key in obj.metadata.iterkeys():
        if key in include:
            print_keys.add(key)
        elif key in exclude:
            hidden_keys.add(key)
        elif key not in non_readable_keys:
            print_keys.add(key)
        elif key not in system_parsed_keys:
            hidden_keys.add(key)

    for key in sorted(list(print_keys)):
        snippet = u"{0}={1}".format(key,
                                    json.dumps(obj.metadata[key], ensure_ascii=False))
        snippets.append(snippet)

    if obj.content_type is not None:
        snippets.insert(0, u"{0}={1}".format('type',
                                             json.dumps(unicode(obj.content_type),
                                                        ensure_ascii=False)))
    if obj.event is not None:
        snippets.insert(0, u"{0}={1}".format('event',
                                             json.dumps(obj.event, ensure_ascii=False)))

    if len(hidden_keys) > 0:
        snippets.append(u"_hidden={0}".format(json.dumps(sorted(list(hidden_keys)), ensure_ascii=False)))

    if file is not None:
        if text is not None:
            print('; '.join(snippets), file=file, end=': ')
            print(text, file=file)
        else:
            print('; '.join(snippets), file=file)
    else:
        if text is not None:
            return u"{0}: {1}".format('; '.join(snippets), text)
        else:
            return '; '.join(snippets)

def print_readably(obj, file=stdout, no_payload=False, include=set(), exclude=set()):
    format_readably(obj, file=file, no_payload=no_payload, include=include, exclude=exclude)
