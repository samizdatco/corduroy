# encoding: utf-8
"""
Corduroy | asynchronous upholstery

Copyright (C) 2012 Samizdat Drafting Co.
A derivative of http://code.google.com/p/couchdb-python by Christopher Lenz

All rights reserved.
BSD Licensed (see LICENSE file for details)
"""

__title__ = 'corduroy'
__version__ = '0.9.0'
__author__ = 'Christian Swinehart'
__license__ = 'BSD'
__copyright__ = 'Copyright 2012 Samizdat Drafting Co.'

__all__ = ['Couch', 'Database', 'Document', 'relax', 'HTTPError', 'Conflict', 
           'NotFound', 'PreconditionFailed', 'ServerError', 'Unauthorized']

from .config import defaults
from .couchdb import Couch, Database, Document
from .exceptions import HTTPError, PreconditionFailed, ServerError, \
                        NotFound, Unauthorized, Conflict

try:
    from tornado import web, gen
    def relax(_func_) :
        """
        A decorator for simplifying async couchdb access from Tornado.
        
        Methods using this decorator will be able to make asynchronous calls to the database
        without needing to provide explicit callbacks (and restoring traditional exception
        handling in the process). It decides between applying tornado's ``@asynchronous`` and
        ``@gen.engine`` decorators as appropriate for a given method.
        
        Its primary function is to intercept yield statements in your request handler and to 
        evaluate them asynchronously (with an automatic callback that it inserts). When the
        database request completes, the callback will resume your function at the yield point
        and pass the received data off with a simple assignment.        
        """
        def _r_e_l_a_x_(*args, **kwargs):
            return gen.engine(_func_)(*args, **kwargs)

        if _func_.__name__ in 'head|get|post|delete|put|options'.split('|'):
            return web.asynchronous(_r_e_l_a_x_)
        else:
            return _r_e_l_a_x_
except ImportError:
    pass
