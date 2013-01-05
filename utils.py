# -*- coding: utf-8 -*-
import select

from datetime import datetime, timedelta

from system import BusinessObject

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
