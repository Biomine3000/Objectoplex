# -*- coding: utf-8 -*-
import signal
import gevent
from gevent.monkey import patch_all

from gevent import Greenlet
from gevent import sleep

import service
from objectoplex import BusinessObject
from objectoplex.services import Service, timeout


class Background(Greenlet):
    def __init__(self):
        Greenlet.__init__(self)

    def _run(self):
        while True:
            sleep(1)
            print '.'


class TemperatureMonitor(Service):
    __service__ = 'temperature_monitor'

    def start(self):
        patch_all() # :D

        self.background = Background()
        gevent.signal(signal.SIGTERM, self.background.kill)
        gevent.signal(signal.SIGINT, self.background.kill)

        self.service_greenlet = Greenlet(super(TemperatureMonitor, self).start)
        gevent.signal(signal.SIGTERM, self.service_greenlet.kill)
        gevent.signal(signal.SIGINT, self.service_greenlet.kill)

        self.background.start()
        self.service_greenlet.start()
        self.service_greenlet.join()

    def sleep(self, seconds):
        sleep(seconds)

    def handle(self, obj):
        self.logger.debug(u"Request {0}".format(obj.metadata))

        # metadata = { 'event': 'services/reply',
        #              'in-reply-to': obj.id }

        # if 'route' in obj.metadata:
        #     metadata['to'] = obj.metadata['route'][0]


service = TemperatureMonitor
