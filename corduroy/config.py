# encoding: utf-8
"""
corduroy.config

Internal state
"""

from __future__ import with_statement
import os, sys
from .atoms import odict, adict, Document

# LATER: add some sort of rcfile support...
# from inspect import getouterframes, currentframe
# _,filename,_,_,_,_ = getouterframes(currentframe())[-1]
# print "from", os.path.dirname(os.path.abspath(filename))

defaults = adict({
            "host":"http://127.0.0.1",
            "port":5984,
            "uuid_cache":50,
            "types":adict({
                "doc":Document,
                "dict":adict
            }),
            "http":adict({
                "max_clients":10,
                "max_redirects":6,
                "timeout":60*60,
                "io_loop":None
            })
         })

try:
    import simplejson as _json
except ImportError:
    import json as _json
class json(object):    
    @classmethod
    def decode(cls, string, **opts):
        """Decode the given JSON string.

        :param string: the JSON string to decode
        :type string: basestring
        :return: the corresponding Python data structure
        :rtype: object
        """
        return _json.loads(string, object_hook=defaults.types.dict, **opts)

    @classmethod
    def encode(cls, obj, **opts):
        """Encode the given object as a JSON string.

        :param obj: the Python data structure to encode
        :type obj: object
        :return: the corresponding JSON string
        :rtype: basestring
        """
        return _json.dumps(obj, allow_nan=False, ensure_ascii=False, encoding='utf-8', **opts)
