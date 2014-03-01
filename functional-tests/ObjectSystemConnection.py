# -*- coding: utf-8 -*-
import socket

from objectoplex import reply_for_object

from robot.api import logger

# from robot.libraries.BuiltIn import BuiltIn
# host = BuiltIn().get_variable_value("${server_host}")
# port = BuiltIn().get_variable_value("${server_port}")

__all__ = ["ObjectSystemConnection"]


class ObjectSystemConnection(object):
    def __init__(self):
        self.sock = None

    def connect_to_server(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, int(port)))

    def send_object(self, obj):
        obj.serialize(self.sock)

    def receive_reply_for(self, obj):
        reply, _ = reply_for_object(obj, self.sock, timeout_secs=15)
        if reply is None:
            raise Exception("Didn't receive reply for object")
        return reply

    def disconnect_from_server(self):
        self.sock.close()
