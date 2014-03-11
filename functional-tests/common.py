# -*- coding: utf-8 -*-
from uuid import uuid4

from objectoplex import BusinessObject
from objectoplex import subscription_object, reply_for_object

from robot.api import logger

__all__ = ["make_subscription_object",
           "make_legacy_subscription_object",
           "make_object_with_natures",
           "set_natures",
           "make_ping_object",
           "object_should_have_key",
           "object_should_have_key_with_value"]

# Subscription
def make_subscription_object():
    result = subscription_object()
    logger.info("Subscription object: " + str(result.metadata))
    return result

# Ping object
def make_ping_object():
    return BusinessObject({'event': 'ping'}, None)

# Natures
def make_object_with_natures(natures):
    return BusinessObject({'natures': natures[0]}, None)

def set_natures(expr):
    ors = expr.split('|')
    return [o.split('&') for o in ors]

# Common test keywords
def object_should_have_key(obj, key):
    if not obj.metadata.has_key(key):
        logger.info("Object metadata: " + str(obj.metadata))
        raise Exception("Object should have had metadata key " + key)

def object_should_have_key_with_value(obj, key, value):
    object_should_have_key(obj, key)

    if obj.metadata[key] != value:
        logger.info("Object metadata: " + str(obj.metadata))
        raise Exception("Object should have had metadata key " + str(key) +
                        " with value " + str(value))

# Legacy support
def make_legacy_subscription_object():
    metadata = {
        'event': 'routing/subscribe',
        'receive-mode': 'all',
        'types': 'all',
        }
    result = BusinessObject(metadata, None)
    logger.info("Subscription object: " + str(result.metadata))
    return result

def make_legacy_no_echo_subscription_object():
    result = make_legacy_subscription_object()
    subscription.metadata['receive-mode'] = 'no_echo'
    logger.info("Subscription object: " + str(result.metadata))
    return result
