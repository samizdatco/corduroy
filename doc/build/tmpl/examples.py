#!/usr/bin/env python
# encoding: utf-8
'''
quickstart.py

Created by Christian Swinehart on 2012-03-04.
Copyright (c) 2012 Samizdat Drafting Co. All rights reserved.
'''

from __future__ import with_statement
import sys
import os
import re
from inspect import getsource
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.filters import GobbleFilter
from pygments.formatters import HtmlFormatter, Terminal256Formatter
from pdb import set_trace as tron
from pprint import pprint

def fmt(fn, css='sample'):
    if not isinstance(fn, basestring):
        lines = getsource(fn).decode('utf-8').split('\n')
        lines = [l[8:] for l in lines]
        code = '\n'.join(lines[1:])
    else:
        code = fn
    
    # source = re.sub(r'\n {8}',r'\n', getsource(fn), re.S)
    # definition, code = source.split('\n',1)
    # code = code.rstrip('\n')
    
    form = HtmlFormatter(cssclass=css)
    # form = Terminal256Formatter()
    lex = PythonLexer()
    return highlight(code, lex, form)
    # print form.get_style_defs()
    
class Quickstart(object):
    def why_init(self):
        db = Database('players')

        application = tornado.web.Application([
            (r'/(.*?)/score)', RankingsUpdater),
        ]).listen(1920)
        tornado.ioloop.IOLoop.instance().start()
        
    def why_jumpy(self):
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
                db.view('leaderboard/highscores', callback=self.got_highscores)
            
            def got_highscores(rows, status):
                self.write(json.dumps(rows))
                self.finish()

    def why_relax(self):        
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

        
        


    def app_boilerplate(self):
        import tornado
        application = tornado.web.Application([
            (r'/hello/([^/]+)', JumpyHello),
            (r'/hi/([^/]+)', RelaxedHello),
        ]).listen(1920)
        tornado.ioloop.IOLoop.instance().start()

    def couch_init(self):
        couchdb = Couch('http://username:pass@127.0.0.1:5984')
        couchdb = Couch('http://127.0.0.1:5984', auth=('username','pass'))
        couchdb = Couch(auth=('username','pass'))

    def couch_interaction(self):
        couch = Couch(auth=('user','pass'))
        try:
            mydb = yield couch.db('mine')
        except NotFound:
            mydb = yield couch.create('mine')

    def db_init(self):
        db = Database('http://127.0.0.1:5984/some_db_name')
        db = Database('some_db_name')

    def doc_get(self):
        db = Database('underlings')
        ollie = yield db.get('lackey-129')
        
        print ollie
        print 'Mr. %(first)s %(last)s can be found at %(office)s.' % ollie

    def doc_save(self):
        del ollie.office
        ollie.education = u'Oxbridge'
        yield db.save(ollie)

        print ollie

    def doc_delete(self):
        try:
            yield db.delete(ollie)
        except Conflict:
            print 'Our doc is stale. Need to refetch and try again.'
            
    def doc_create(self):
        newdoc = {'_id':'lackey-130', 'first':'Angela', 'last':'Heaney'}
        yield db.save(newdoc)
        
        print newdoc._rev

    def doc_create_anon(self):
        anon = {'first':'Julius', 'last':'Nicholson'}
        yield db.save(anon)
        
        print anon._id, anon._rev

    def batch_get(self):
        db = Database('backbench')
        doc_ids = ['ballentine', 'holhurst', 'swain']
        docs = yield db.get(doc_ids)
        
        print docs[0]
        
    def batch_save(self):
        claire, geoff, ben = docs
        
        claire.standing = u'not standing'
        geoff._deleted = True
        ben.newsnight = {'paxman':1, 'swain':0}
        yield db.save([claire, geoff, ben])

    def resolution_success(self):
        # create a pair of new docs
        docs = [{'_id':'first', 'n':1}, {'_id':'second', 'n':2}]
        conflicts = yield db.save(docs)
        print conflicts

    def resolution_conflicts(self):
        # create a conflict by deleting the rev and re-saving
        del docs[1]._rev
        conflicts = yield db.save(docs)
        print conflicts
        
    def resolution_merge(self):
        def mergefn(local_doc, server_doc):
            # just copy over the rev (a.k.a. not a real strategy)
            local_doc._rev = server_doc._rev
            return local_doc

        conflicts = yield db.save(docs)
        if conflicts.pending:
            print 'pre-merge: ', len(conflicts.pending)
            yield conflicts.resolve(mergefn)
            print 'post-merge:', len(conflicts.pending)

        
    def resolution_before(self):
        def mergefn(local_doc, server_doc):
            local_doc._rev = server_doc._rev
            return local_doc

        yield db.save(docs, merge=mergefn)

    def resolution_with_force(self):
        yield db.save(docs, force=True)
        


    def query_simple(self):
        db = Database('dosac')
        rows = yield db.view('employees/byname')
        print rows

    def query_rowbits(self):
        for row in rows:
            print 'key:%s\t| id:%s' % (row.key, row.id)
        
    def query_hugh(self):
        yield db.view('employees/byname', key='abbott')

    def query_few(self):
        yield db.view('employees/byname', keys=['coverley', 'murray'])
        
    def query_c_words(self):
        rows = yield db.view('employees/byname', inclusive_end=False,
                                                 startkey='c', endkey='d')
        print [row.key for row in rows]

    def query_hugh_further(self):
        rows = yield db.view('employees/byname', key='abbott', include_docs=True)
        print rows[0].doc

    def query_no_reduce(self):
        for row in (yield db.view('chronological/counts', reduce=False)):
            print row.key
            
    def query_reduce(self):
        for row in (yield db.view('chronological/counts', group=True)):
            print "%(key)s → %(value)i" % row

    def query_reduce_group(self):
        for row in (yield db.view('chronological/counts', group_level=2)):
            print "%(key)s → %(value)i" % row
        
    def format_show(self):
        db = Database('mannion')
        response = yield db.show('records/csv', '1978-wotw', include_titles=True)
        print response.headers['Content-Type']
        
    def format_show_more(self):
        print response.body
        
    def format_list(self):
        db = Database('tucker')
        response = yield db.list('agenda/html', 'nomfup', 
                                 key='n', limit=3, descending=True)
        print response.body
                                 
        
    def format_list_elsewhere(self):
        response = yield db.list('agenda/html', 'bollockings/byday', 
                                 key='2007-10-13')
        print response.body
        
    def couch_interaction_brief(self):
        mydb = yield couch.db('mine', create_if_missing=True)

    def relax_pseudocode(self):
        def relax_callback(data, status):
            if status.exception:
                raise status.exception # (in the context of your function)
            else:
                return data # (and assign it to the yield's lvalue)

    def example_callback(self):
        def mycallback(data, status):
            pass



    def replicate(self):
        couch = Couch()
        yield couch.replicate('olddb', 'newdb', create_target=True)


    def replicate_push(self):
        local_db = Database('localdb')
        remote_db = Database('http://elsewhere.com:5984/remotedb')
        yield local_db.push(remote_db)
        yield local_db.pull(remote_db)

    def replicator(self):        
        couch = Couch()
        repl = { "_id":"local-to-remote",
                 "source":"localdb", 
                 "target":"http://elsewhere.com:5984/remotedb",
                 "continuous":True }
        yield couch.replicator.save(repl)

        repl_doc = yield couch.replicator.get('local-to-remote')
        print json.dumps(repl_doc, indent=2)

    def replicator_convenience(self):
        local_db = Database('localdb')
        yield local_db.push(remote_db, 
                            "http://elsewhere.com:5984/remotedb", 
                            continuous=True, 
                            _id='local-to-remote')

    def replicator_star(self):
        repl = { "_id":"local-to-remote",
                 "target":"http://elsewhere.com:5984/remotedb",
                 "continuous":True }
        yield local_db.push(**repl)
        
    def changes_seq(self):
        db = Database('watched')
        info = yield db.info()
        print info.update_seq

    def changes_everything(self):
        changes = yield db.changes()
        print "seq: %i (%i changes)"% (changes.last_seq, len(changes.results))
        
    def changes_bracket(self):
        changes = yield db.changes(since=11500, limit=5)
        print "seq: %i (%i changes)"% (changes.last_seq, len(changes.results))

    def changes_feed(self):
        def listener(last_seq, changes):
            pass
        
        feed = db.changes(since=11500, feed='continuous', callback=listener)
        
    def changes_stop(self):
        feed.stop()


    def cords_boilerplate(self):
        from corduroy import Database, NotFound, relax
        people_db = Database('people') # i.e., http://127.0.0.1:5984/people
        
    def jumpy_app(self):
        class JumpyHello(tornado.web.RequestHandler):
            @tornado.web.asynchronous
            def get(self, user_id):
                # Request the corresponding user's doc. This will return
                # immediately and control will leave this method. Later on
                # the got_user_doc callback will be invoked with the 
                # response and a status object as arguments.
                people_db.get(user_id, callback=self.got_user_doc)

            def got_user_doc(self, doc, status):
                # Generate output based on the db's response
                if status.ok:
                    self.write('hello %s %s'%(doc['first'],doc['last']))
                elif status.error is NotFound:
                    self.write('hello whoever you are')
                else:
                    raise status.exception
                self.finish()


    def relaxed_app(self):
        class RelaxedHello(tornado.web.RequestHandler):
            @relax
            def get(self, user_id):
                try:
                    doc = yield people_db.get(user_id)
                    self.write('hello %s %s'%(doc['first'],doc['last']))
                except NotFound:
                    self.write('hello whoever you are')
                self.finish()


examples = {}
for nm, fn in Quickstart.__dict__.items():
    if nm.startswith('_'): continue
    examples[nm] = fmt(fn)

def main():    
    pprint(examples)
    print examples['first']

if __name__ == '__main__':
    main()

