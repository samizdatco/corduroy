#!/usr/bin/env python
# encoding: utf-8
"""
corduroy.tests.testutil
"""

import random
import sys
from corduroy import *

    
# from couchdb-python
class TempDatabaseMixin(object):
    temp_dbs = None
    _db = None

    def setUp(self):
        self.server = Couch(full_commit=False)
        super(TempDatabaseMixin, self).setUp()

    def tearDown(self):
        if self.temp_dbs:
            for name in self.temp_dbs:
                try:
                    self.server.delete(name)
                except NotFound:
                    pass
        super(TempDatabaseMixin, self).tearDown()

    def temp_db(self):
        if self.temp_dbs is None:
            self.temp_dbs = {}
        # Find an unused database name
        while True:
            name = 'c_o_r_d_u_r_o_y/%d' % random.randint(0, sys.maxint)
            if name not in self.temp_dbs:
                break
        db = self.server.create(name)
        self.temp_dbs[name] = db
        return name, db

    def del_db(self, name):
        del self.temp_dbs[name]
        self.server.delete(name)

    @property
    def db(self):
        if self._db is None:
            name, self._db = self.temp_db()
        return self._db


try:
    import tornado.testing
    from tornado import ioloop

    class AsyncTestCase(TempDatabaseMixin, tornado.testing.AsyncTestCase):
        def get_new_ioloop(self):
            return ioloop.IOLoop.instance()

        def stop(self, *_arg, **kwargs):
            '''Stops the ioloop, causing one pending (or future) call to wait()
            to return.

            Keyword arguments or a single positional argument passed to stop() are
            saved and will be returned by wait().
            '''
            assert not _arg or not kwargs
            self.__stop_args = kwargs or _arg
            if self.__running:
                self.io_loop.stop()
                self.__running = False
            self.__stopped = True
except ImportError:
    class AsyncTestCase(object):
        pass
