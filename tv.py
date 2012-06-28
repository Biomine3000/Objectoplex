#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import logging

from system import *

logger = logging.getLogger("tv")

def make_registration_object(client_name, user_name):
    metadata = {
        'event': 'clients/register',
        'receive': 'all',
        'subscriptions': 'all',
        'name': client_name,
        'user': user_name
        }
    return BusinessObject(metadata, None)

 # - "clients/register" - register a client to the server. possible services
 #      implemented by the client will be registered separately as "services/register" events.
 #     • "receive" - one of: 
 #        * "none": nothing will be sent to the client by server
 #        * "all": everything will be sent
 #        * "no_echo": everything but objects sent by client itself  
 #        * "events_only": events only (recall that events may include no or arbitrary CONTENT)
 #     • "subscribe" - a json array of CONTENT types that client is willing to receive. Note that
 #       this does not consider event types; all events will be received, irrespective of their type
 #       (and even irrespective of any potential content included as payload!) 
 #        * "all" to receive everything (default); this is not an json array, but an string literal instead
 #        * "none" to receive nothing; ; this is not an json array, but an string literal instead
 #        * trivial bash-style wildcards could be supported at some point  
 #     • "name" - name of the client program, e.g. "java-tv". optional 
 #        and purely informational; not used by protocol
 #     • "user" - name of the user running the program. optional and purely 
 #        informational; not used by protocol 
 #     • "unique-routing-id" - a client must have an id to receive targeted messages from services.
 #       if no id, a server will provide one (maybe also informing the client
 #       of its new id). Maybe client could include the id in each request, instead of registering
 #       it t  
 #     • "routing-ids": a list of additional id:s based on which events shall be routed
 #        to the client
