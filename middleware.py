# -*- coding: utf-8 -*-
import hashlib
import logging

logger = logging.getLogger('middleware')

class Middleware(object):
    def handle(self, message, client, clients):
        """
        Return None to signify that this message is handled and doesn't need
        to be passed on to the next middleware.
        """
        return message

    def register(self, client):
        pass

    def unregister(self, client):
        pass


class ChecksumMiddleware(Middleware):
    def handle(self, message, *args):
        if 'sha1' not in message.metadata and message.size > 0:
            message.metadata['sha1'] = hashlib.sha1(message.payload).hexdigest()
        return message

class MultiplexingMiddleware(Middleware):
    def handle(self, message, sender, clients):
        for client in clients:
            client.send(message, sender)
        return None
