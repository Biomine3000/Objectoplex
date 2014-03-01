# -*- coding: utf-8 -*-
from objectoplex import BusinessObject
from objectoplex import subscription_object, reply_for_object

__all__ = ["make_subscription_object",
           "make_object_with_natures"]

def make_subscription_object(*args):
    return subscription_object(list(args))

def make_object_with_natures(natures):
    return BusinessObject({'natures': natures}, None)
