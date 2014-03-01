# -*- coding: utf-8 -*-
from objectoplex import subscription_object, reply_for_object

__all__ = ["make_subscription_object"]

def make_subscription_object(*args):
    return subscription_object(list(args))
