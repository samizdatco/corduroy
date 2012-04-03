==================================
Corduroy · asynchronous upholstery
==================================
:project: http://samizdat.cc/corduroy
:code: http://github.com/samizdatco/corduroy

About
=====

Corduroy provides a python-friendly wrapper around CouchDB’s HTTP-based API.
Behind the scenes it hooks into the asynchronous i/o routines from your choice
of `Tornado <http://www.tornadoweb.org/>`_ or the 
`Requests <http://docs.python-requests.org/>`_ & `Gevent <http://gevent.org/>`_ modules.

Using corduroy you can query the database without blocking your server’s event
loop, making it ideal for `CouchApp <http://couchapp.org/page/index>`_ micro-middleware 
or scripted batch operations.

Usage
=====

As a real world(ish) example of working with Corduroy, consider this pair of
Tornado event handlers which update a url-specifed document then query a view.
The first uses explicit callbacks to resume execution after each response from
the database is received::

    db = Database('players')
    class RankingsUpdater(tornado.web.RequestHandler):
        @tornado.web.asynchronous
        def post(self, player_id):
            self.new_score = int(self.request.body)
            db.get(player_id, callback=self.got_player)

        def got_player(doc, status):
            doc.score = self.new_score
            db.save(doc, callback=self.saved_player)

        def saved_player(conflicts, status):
            db.view('leaderboard/highscores', 
                     callback=self.got_highscores)

        def got_highscores(rows, status):
            self.write(json.dumps(rows))
            self.finish()

An alternative syntax is available (when using Tornado) through the use of the
@relax decorator. Instead of defining callbacks for each database operation,
the library can be called as part of a yield expression.

Tornado’s `generator <http://www.tornadoweb.org/documentation/gen.html>`_ module 
will intercept these yields and provide a callback automatically. The result is 
code that looks quite sequential but will still execute asyncronously::

    class RankingsUpdater(tornado.web.RequestHandler):
        @relax
        def post(self, player_id):
            # update this player's score
            doc = yield db.get(player_id)
            doc.score = int(self.request.body)
            yield db.save(doc)

            # return the new rankings
            highscores = yield db.view('leaderboard/highscores')
            self.write(json.dumps(highscores))
            self.finish()

For a gentle introduction to Corduroy (and CouchDB in general), take a look at
the `Guide <http://samizdat.cc/corduroy/guide/>`_. Documentation for all of Corduroy’s 
module-level classes can be found in the `Reference <http://samizdat.cc/corduroy/ref>`_ 
section.

Installation
============

Automatic Installation
----------------------

Corduroy can be found on PyPi and can be installed with your choice of pip or
easy_install.

Manual Installation
-------------------

Download `corduroy-0.9.0.tar.gz <http://samizdat.cc/corduroy/dist/corduroy-0.9.0.tar.gz>`_::

    tar xvzf corduroy-0.9.0.tar.gz
    cd corduroy-0.9.0
    python setup.py install

Dependencies
------------

If you’re writing a Tornado app, Corduroy can use its pure-python HTTP client
by installing with::

    pip install corduroy tornado

Or if you’d prefer the libcurl-based client (which supports pooling and other
niceties), use::

    pip install corduroy tornado pycurl

If pycurl complains (I’m looking at you, OS X), try::

    env ARCHFLAGS="-arch x86_64" pip install pycurl

Gevent users can install with::

    pip install corduroy requests gevent

The library can also be used with plain-old blocking i/o::

    pip install corduroy requests

License
=======

Corduroy is released under the BSD license. Use it freely and in good health.

Acknowledgments
===============

Corduroy is derived from Christopher Lenz’s excellent `couchdb-python
<http://code.google.com/p/couchdb-python>`_ module and inherits much of its
API (and most of its test cases) from that codebase. It is also indebted to
Eric Naeseth’s mind-expanding `Swirl <http://code.naeseth.com/swirl/>`_
library which first acquainted me with the idea of using generators to
simulate sequential code.
