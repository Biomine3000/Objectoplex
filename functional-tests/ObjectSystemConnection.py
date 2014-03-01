# -*- coding: utf-8 -*-
import socket

from objectoplex import reply_for_object
from objectoplex.utils import read_object_with_timeout

from robot.api import logger

# from robot.libraries.BuiltIn import BuiltIn
# host = BuiltIn().get_variable_value("${server_host}")
# port = BuiltIn().get_variable_value("${server_port}")

__all__ = ["ObjectSystemConnection"]

TIMEOUT = 1


class ObjectSystemConnection(object):
    def __init__(self):
        self.sock = None

    def connect_to_server(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, int(port)))

    def send_object(self, obj):
        logger.info("Sending object: " + str(obj.metadata))
        obj.serialize(self.sock)

    def receive_reply_for(self, obj):
        reply, _ = reply_for_object(obj, self.sock, timeout_secs=TIMEOUT)
        if reply is None:
            raise Exception("Didn't receive reply for object")
        return reply

    def should_receive_reply_for(self, obj):
        return self.receive_reply_for(obj)

    def should_not_receive_reply_for(self, obj):
        for i in xrange(TIMEOUT):
            reply, _ = reply_for_object(obj, self.sock, timeout_secs=1.0)
            if reply is not None:
                raise Exception("Unexpectedly received reply for object")

    def should_receive_object(self, obj):
        for i in xrange(TIMEOUT):
            incoming = read_object_with_timeout(self.sock, timeout_secs=1.0)
            if incoming is not None:
                logger.info("Received " + str(incoming.metadata))
            else:
                logger.info("Received " + str(incoming))
            if incoming is not None and incoming.id == obj.id:
                return
        raise Exception("Didn't receive expected object: " + str(obj.metadata))

    def should_not_receive_object(self, obj):
        for i in xrange(TIMEOUT):
            incoming = read_object_with_timeout(self.sock, timeout_secs=1.0)
            if incoming is not None:
                logger.info("Received " + str(incoming.metadata))
            else:
                logger.info("Received " + str(incoming))
            if incoming is not None and incoming.id == obj.id:
                raise Exception("Unexpectedly received object: " + str(obj.metadata))

    def disconnect_from_server(self):
        self.sock.close()
