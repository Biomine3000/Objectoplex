# -*- coding: utf-8 -*-
from subprocess import call
from os import environ as env

from Tkinter import *

from objectoplex import BusinessObject
from objectoplex.services import Service

class App(object):
    def __init__(self, master, command, url, timeout, logger):
        self.master = master
        self.command = command
        self.url = url
        self.logger = logger

        self.master.bind("<Escape>", self.done)

        self.polled_times = 0


        frame = Frame(master)
        frame.pack()

        self.label = Label(frame, text="Press ESC to cancel opening, RET/SPACE to open",
                           anchor=W, justify=LEFT)
        self.label.pack(side=TOP)
        self.button = Button(frame, text="Open URL %s" % self.url, command=self.open_url)
        self.button.pack(side=BOTTOM)
        self.button.focus_set()

        self.master.after(timeout, self.done)

    def open_url(self):
        self.logger.debug("Trying to open URL %s" % self.url)
        call([self.command, self.url])
        self.logger.info("Opened URL %s" % self.url)
        # print "would open url " + self.url
        self.done()

    def done(self, *args):
        self.master.destroy()
        self.logger.debug("Closing GUI")


class UrlOpener(Service):
    __service__ = 'url_opener'

    def __init__(self, *args, **kwargs):
        super(UrlOpener, self).__init__(*args, **kwargs)

        self.logger.info("Accepted parameters (key=value): command, user, timeout (in ms)")

        self.user = self.args.get('user', env['USER'])
        self.command = self.args.get('command', 'open')
        self.timeout = int(self.args.get('timeout', '1500'))
        self.logger.info("Using command \"%s\" for opening URLs" % self.command)
        self.logger.info("Opening URLs from user \"%s\"" % self.user)
        self.logger.info("Using %ims for GUI timeout" % self.timeout)

    def handle(self, obj):
        url = unicode(obj.payload)

        root = Tk()
        app = App(root, self.command, url, self.timeout, self.logger)
        root.mainloop()

    def should_handle(self, obj):
        if super(UrlOpener, self).should_handle(obj):
            if obj.metadata.get('user', None) == self.user:
                return True
            self.logger.debug("User doesn't match: \"%s\" != \"%s\"" %
                              (obj.metadata.get('user', None), self.user))
        return False


service = UrlOpener
