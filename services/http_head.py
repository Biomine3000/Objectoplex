# -*- coding: utf-8 -*-
import traceback
import json

import requests

from system import BusinessObject
from service import Service, timeout


class HttpHead(Service):
    __service__ = 'http_head'

    def handle(self, obj):
        self.logger.debug(u"Request {0}".format(obj.metadata))

        try:
            if 'url' not in obj.metadata:
                metadata = {'event': 'services/reply',
                            'in-reply-to': obj.id,
                            'error': "URL for head request not found in metadata (attribute 'url')"}
                if 'route' in obj.metadata:
                    metadata['to'] = obj.metadata['route'][0]
                reply = BusinessObject(metadata, None)
                self.logger.debug(u"Error reply {0}".format(reply.metadata))
                return reply


            payload = {}
            r = requests.head(obj.metadata['url'])
            for k, v in r.headers.iteritems():
                payload[k] = v
            payload['status_code'] = r.status_code

            metadata = {'event': 'services/reply',
                        'in-reply-to': obj.id,
                        'type': 'text/json; charset=utf-8'}
            if 'route' in obj.metadata:
                metadata['to'] = obj.metadata['route'][0]

            payload = bytearray(json.dumps(payload, ensure_ascii=False),
                                encoding='utf-8')
            metadata['size'] = len(payload)

            reply = BusinessObject(metadata, payload)
            self.logger.debug(u"Reply {0}".format(reply.metadata))
            return reply
        except Exception, e:
            traceback.print_exc()
            self.logger.error(u"{0}".format(e))

service = HttpHead
