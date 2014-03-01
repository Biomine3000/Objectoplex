# -*- coding: utf-8 -*-
from objectoplex import BusinessObject
from objectoplex import subscription_object, reply_for_object

from robot.api import logger

__all__ = ["make_subscription_object",
           "make_object_with_natures",
           "set_natures"]

def make_subscription_object(natures=[]):
    result = subscription_object(natures)
    logger.info("Subscription object: " + str(result.metadata))
    return result

def make_object_with_natures(natures):
    return BusinessObject({'natures': natures[0]}, None)

def set_natures(expr):
    ors = expr.split('|')
    return [o.split('&') for o in ors]
