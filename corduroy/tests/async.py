#!/usr/bin/env python
# encoding: utf-8
"""
corduroy.tests.async
"""

from __future__ import with_statement
import sys
import os
import time
from datetime import datetime, timedelta
import unittest
import shutil
import tempfile
import threading
import unittest
import urlparse

from StringIO import StringIO
from testutil import TempDatabaseMixin, AsyncTestCase
import corduroy
from corduroy import *
from corduroy.atoms import *
from pdb import set_trace as tron

# async transcription of the tests in blocking.py
class AsyncCouchTests(AsyncTestCase):

    def test_basics(self):
        self.assertEqual(0, len(self.db))


        # list all the databases
        server = Couch()
        server.all_dbs(callback=self.stop)
        dbs, status = self.wait()
        # _users and _replicator should be in there, but might not be.
        # what to test then...

    
        # create a document
        data = {'_id':'0', 'a': 1, 'b': 1}
        self.db.save(data, callback=self.stop)
        _, status = self.wait()
        
        self.assertEqual('0', data['_id'])
        assert '_rev' in data
        self.db.get('0', callback=self.stop)
        doc, _ = self.wait()
        # doc = self.db['0']
        self.assertEqual('0', doc['_id'])
        self.assertEqual(data['_rev'], doc['_rev'])
        self.assertEqual(1, len(self.db))
    
        # delete a document
        self.db.delete(doc, callback=self.stop)
        _, status = self.wait()
    
        self.db.get('0', callback=self.stop)
        _, status = self.wait()
        self.assertEqual(NotFound, status.error)
    
        # test _all_docs
        num = 4
        for i in range(num):
            self.db[str(i)] = {'a': i + 1, 'b': (i + 1) ** 2}
        self.assertEqual(num, len(self.db))
        
        self.db.get(callback=self.stop)
        all_docs, status = self.wait()
        for doc in all_docs:
            assert int(doc['_id']) in range(4)
    
        # test a simple query
        query = """function(doc) {
            if (doc.a==4)
                emit(null, doc.b);
        }"""
        self.db.query(query, callback=self.stop)
        result, status = self.wait()
        self.assertEqual(1, len(result))
        self.assertEqual('3', result[0].id)
        self.assertEqual(16, result[0].value)
    
        # modify a document, and redo the query
        doc = self.db['0']
        doc['a'] = 4
        self.db['0'] = doc
        self.db.query(query, callback=self.stop)
        result, status = self.wait()
        self.assertEqual(2, len(result))
    
        # add more documents, and redo the query again
        self.db.save({'a': 3, 'b': 9})
        self.db.save({'a': 4, 'b': 16})
        self.db.query(query, callback=self.stop)
        result, status = self.wait()
        self.assertEqual(3, len(result))
        self.assertEqual(6, len(self.db))
    
        # delete a document, and redo the query once more
        del self.db['0']
        self.db.query(query, callback=self.stop)
        result, status = self.wait()
        self.assertEqual(2, len(result))
        self.assertEqual(5, len(self.db))
    
    def test_conflict_detection(self):
        doc1 = {'a': 1, 'b': 1, '_id':'foo'}
        self.db.save(doc1, callback=self.stop)
        _, status = self.wait()
        self.assertTrue(status.ok)
        
        # self.db['foo'] = doc1
        self.db.get('foo', callback=self.stop)
        doc2, status = self.wait()
        self.assertTrue(status.ok)
        
        self.assertEqual(doc1['_id'], doc2['_id'])
        self.assertEqual(doc1['_rev'], doc2['_rev'])
    
        # make conflicting modifications
        doc1['a'] = 2
        doc2['a'] = 3
        self.db.save(doc1, callback=self.stop)
        _, status = self.wait()
        self.assertTrue(status.ok)
        
        self.db.save(doc2, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(status.error is Conflict)
        self.assertIn(doc2['_id'], conflicts.pending)
        
        # try submitting without the revision info
        del doc2['_rev']
        self.db.save(doc2, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(status.error is Conflict)
    
        # resolve the conflict by copying the _rev
        conflicts.overwrite(callback=self.stop)
        result, status = self.wait()
    
        self.db.delete(doc1, callback=self.stop)
        result, status = self.wait()
        self.db.save(doc2, callback=self.stop)
        _, status = self.wait()
        self.assertTrue(status.ok)

    def test_conflict_resolution(self):
        def mergefn(local_doc, server_doc):
           local_doc.update(server_doc)
           return local_doc     

        db = self.db
        docs = [{'bar':1},{'bar':2},{'bar':3}]
        db.save(docs, callback=self.stop) # docs -> [{'_id':'some-uuid','_rev':'some-hash', 'bar':1}, ...]
        conflicts, status = self.wait()
        self.assertEqual('<Success: 3 docs updated>', conflicts.__repr__())
        
        # create a conflict
        del docs[0]['_rev']
        db.save(docs, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEqual(1, len(conflicts.pending))
        self.assertIn(docs[0]['_id'], conflicts.pending)
        
        # brute-force _rev copy our way out of this mess
        conflicts.overwrite(callback=self.stop)
        conflicts, status = self.wait()
        self.assertFalse(conflicts.pending)
        self.assertEqual(3, len(conflicts.resolved))
        
        # make sure the up-to-date copies really are
        current = conflicts.resolved.values()
        db.save(current, callback=self.stop)
        conflicts, status = self.wait()
        self.assertFalse(conflicts.pending)
        
        # create a conflict and overwrite it in one shot
        del docs[0]['_rev']
        db.save(docs, force=True, callback=self.stop)
        conflicts, status = self.wait()
        self.assertFalse(conflicts.pending)
        
        # similar one-shot overwrite but with a single doc
        doc = conflicts.resolved.values()[0]
        del doc['_rev']
        doc['foo'] = 2600
        db.save(doc, force=True, callback=self.stop)
        conflicts, status = self.wait()
        self.assertEqual(1, len(conflicts.resolved))

        # make sure it really wrote, including the attr change
        db.get(doc['_id'], callback=self.stop)
        redoc, status = self.wait()
        self.assertEqual(dict(doc), dict(redoc))
        
    def test_lots_of_docs(self):
        num = 100 # Crank up manually to really test
        for i in range(num): 
            self.db.save({'_id':str(i), 'integer': i, 'string': str(i)}, callback=self.stop)
            _, status = self.wait()
            self.assertTrue(status.ok)
        self.assertEqual(num, len(self.db))
    
        query = """function(doc) {
            emit(doc.integer, null);
        }"""
        self.db.query(query, callback=self.stop)
        results, status = self.wait(timeout=90)
        
        self.assertEqual(num, len(results))
        for idx, row in enumerate(results):
            self.assertEqual(idx, row.key)
    
        self.db.query(query, descending=True, callback=self.stop)
        results, status = self.wait(timeout=90)
        self.assertEqual(num, len(results))
        for idx, row in enumerate(results):
            self.assertEqual(num - idx - 1, row.key)
    
    def test_multiple_rows(self):
        self.db['NC'] = {'cities': ["Charlotte", "Raleigh"]}
        self.db['MA'] = {'cities': ["Boston", "Lowell", "Worcester",
                                    "Cambridge", "Springfield"]}
        self.db['FL'] = {'cities': ["Miami", "Tampa", "Orlando",
                                    "Springfield"]}
    
        query = """function(doc){
            for (var i = 0; i < doc.cities.length; i++) {
                emit(doc.cities[i] + ", " + doc._id, null);
            }
        }"""
        self.db.query(query, callback=self.stop)
        results, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEqual(11, len(results))
        self.assertEqual("Boston, MA", results[0].key);
        self.assertEqual("Cambridge, MA", results[1].key);
        self.assertEqual("Charlotte, NC", results[2].key);
        self.assertEqual("Lowell, MA", results[3].key);
        self.assertEqual("Miami, FL", results[4].key);
        self.assertEqual("Orlando, FL", results[5].key);
        self.assertEqual("Raleigh, NC", results[6].key);
        self.assertEqual("Springfield, FL", results[7].key);
        self.assertEqual("Springfield, MA", results[8].key);
        self.assertEqual("Tampa, FL", results[9].key);
        self.assertEqual("Worcester, MA", results[10].key);
    
        # Add a city and rerun the query
        doc = self.db['NC']
        doc['cities'].append("Wilmington")
        self.db['NC'] = doc
        self.db.query(query, callback=self.stop)
        results, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEqual(12, len(results))
        self.assertEqual("Wilmington, NC", results[10].key)
    
        # Remove a document and redo the query again
        del self.db['MA']
        self.db.query(query, callback=self.stop)
        results, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEqual(7, len(results))
        self.assertEqual("Charlotte, NC", results[0].key);
        self.assertEqual("Miami, FL", results[1].key);
        self.assertEqual("Orlando, FL", results[2].key);
        self.assertEqual("Raleigh, NC", results[3].key);
        self.assertEqual("Springfield, FL", results[4].key);
        self.assertEqual("Tampa, FL", results[5].key);
        self.assertEqual("Wilmington, NC", results[6].key)
    
    def test_large_docs(self):
        size = 100
        longtext = '0123456789\n' * size
        for i in xrange(6):
            self.db.save({'longtext': longtext}, callback=self.stop)
            _, status = self.wait()
            self.assertTrue(status.ok)
    
        query = """function(doc) {
            emit(null, doc.longtext);
        }"""
        self.db.query(query, callback=self.stop)
        results, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEqual(6, len(results))
    
    def test_utf8_encoding(self):
        texts = [
            u"1. Ascii: hello",
            u"2. Russian: На берегу пустынных волн",
            u"3. Math: ∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i),",
            u"4. Geek: STARGΛ̊TE SG-1",
            u"5. Braille: ⡌⠁⠧⠑ ⠼⠁⠒  ⡍⠜⠇⠑⠹⠰⠎ ⡣⠕⠌"
        ]
        for idx, text in enumerate(texts):
            doc = {'text': text, '_id':str(idx)}
            self.db.save(doc, callback=self.stop)
            _, status = self.wait()
            self.assertTrue(status.ok)
            self.assertEqual(text, doc['text'])
            
        query = """function(doc) {
            emit(doc.text, null);
        }"""
        self.db.query(query, callback=self.stop)
        results, status = self.wait()
        self.assertTrue(status.ok)
        for idx, row in enumerate(results):
            self.assertEqual(texts[idx], row.key)
    
    def test_design_docs(self):
        for i in range(50): 
            self.db[str(i)] = {'integer': i, 'string': str(i)}
        self.db['_design/test'] = {'views': {
            'all_docs': {'map': 'function(doc) { emit(doc.integer, null) }'},
            'no_docs': {'map': 'function(doc) {}'},
            'single_doc': {'map': 'function(doc) { if (doc._id == "1") emit(null, 1) }'}
        }}
        self.db.view('test/all_docs', callback=self.stop)
        all_view, status = self.wait()
        self.assertTrue(status.ok)
        for idx, row in enumerate(all_view):
            self.assertEqual(idx, row.key)
    
        self.db.view('test/no_docs', callback=self.stop)
        no_view, status = self.wait()
        self.assertTrue(status.ok)            
        self.assertEqual(0, len(no_view))
        
        self.db.view('test/single_doc', callback=self.stop)
        single_view, status = self.wait()
        self.assertTrue(status.ok)            
        self.assertEqual(1, len(single_view))
    
    def test_collation(self):
        values = [
            None, False, True,
            1, 2, 3.0, 4,
            'a', 'A', 'aa', 'b', 'B', 'ba', 'bb',
            ['a'], ['b'], ['b', 'c'], ['b', 'c', 'a'], ['b', 'd'],
            ['b', 'd', 'e'],
            {'a': 1}, {'a': 2}, {'b': 1}, {'b': 2}, {'b': 2, 'c': 2},
        ]
        self.db['0'] = {'bar': 0}
        for idx, value in enumerate(values):
            self.db[str(idx + 1)] = {'foo': value}
    
        query = """function(doc) {
            if(doc.foo !== undefined) {
                emit(doc.foo, null);
            }
        }"""
        self.db.query(query, callback=self.stop)
        view, status = self.wait()
        self.assertTrue(status.ok)
    
        rows = iter(view)
        self.assertEqual(None, rows.next().value)
        for idx, row in enumerate(rows):
            self.assertEqual(values[idx + 1], row.key)
    
        self.db.query(query, descending=True, callback=self.stop)
        rows, status = self.wait()
        self.assertTrue(status.ok)
        for idx, row in enumerate(rows):
            if idx < len(values):
                self.assertEqual(values[len(values) - 1- idx], row.key)
            else:
                self.assertEqual(None, row.value)
    
        for value in values:
            self.db.query(query, key=value, callback=self.stop)
            rows, status = self.wait()
            self.assertTrue(status.ok)
            self.assertEqual(1, len(rows))
            self.assertEqual(value, rows[0].key)

    def test_missing(self):
        docs = [{'name':'Eddie Mars'}, {'name':'Carmen Sternwood'}, {'name':'Terry Lennox'}]
        self.db.save(docs, callback=self.stop)
        conflicts, status = self.wait()
        doc_ids = [doc['_id'] for doc in docs]
        
        self.db.get(doc_ids, callback=self.stop)
        redocs, status = self.wait()
        self.assertTrue(status.ok)
        
        self.db.get(doc_ids+["nonexistent-doc-id"], callback=self.stop)
        redocs, status = self.wait()
        self.assertIsNone(redocs[-1])
        
        self.db.get("nonexistent-doc-id", callback=self.stop)
        nodoc, status = self.wait()

        # Couch().get('mememe', create_if_missing=True, callback=self.stop)
        # nodb, status = self.wait()
        # tron()
        mememe = Couch().db('mememe', create_if_missing=True)
        Couch().delete('mememe')

class AsyncCouchTestCase(AsyncTestCase):

    def test_init_with_resource(self):
        res = corduroy.io.Resource('http://127.0.0.1:5984')
        serv = Couch(url=res)
        serv.config(callback=self.stop)
        cfg, status = self.wait()
        self.assertTrue(status.ok)

    def test_exists(self):
        Couch().version(callback=self.stop)
        _, status = self.wait()
        self.assertTrue(status.ok)

        Couch('http://localhost:9879').version(callback=self.stop)
        _, status = self.wait()
        self.assertFalse(status.ok)

    def test_repr(self):
        repr(self.server)

    def test_server_vars(self):
        self.server.version(callback=self.stop)
        version, status = self.wait()        
        self.assertTrue(isinstance(version, basestring))
        
        self.server.config(callback=self.stop)
        config, status = self.wait()        
        self.assertTrue(isinstance(config, dict))
        
        self.server.tasks(callback=self.stop)
        tasks, status = self.wait()
        self.assertTrue(isinstance(tasks, list))
        
    def test_server_stats(self):
        self.server.stats(callback=self.stop)
        stats, status = self.wait()        
        self.assertTrue(isinstance(stats, dict))
        
        stats = self.server.stats('httpd/requests',callback=self.stop)
        stats, status = self.wait()        
        self.assertTrue(isinstance(stats, dict))
        self.assertTrue(len(stats) == 1 and len(stats['httpd']) == 1)

    def test_get_db_missing(self):
        self.server.db('c_o_r_d_u_r_o_y/missing', callback=self.stop)
        db, status = self.wait()
        self.assertEquals(NotFound, status.error)
        self.assertEquals(None, db)

    def test_create_db_conflict(self):
        name, db = self.temp_db()
        self.server.create(name, callback=self.stop)
        db, status = self.wait()
        self.assertEquals(PreconditionFailed, status.error)

    def test_delete_db(self):
        name, db = self.temp_db()
        assert name in self.server
        self.server.delete(name, callback=self.stop)
        resp, status = self.wait()
        self.assertTrue(status.ok)

        self.server.db(name, callback=self.stop)
        _, status = self.wait()
        self.assertEquals(NotFound, status.error)

    def test_delete_db_missing(self):
        self.server.delete('c_o_r_d_u_r_o_y/missing', callback=self.stop)
        _, status = self.wait()
        self.assertEquals(NotFound, status.error)

    def test_replicate(self):
        aname, a = self.temp_db()
        bname, b = self.temp_db()
        adoc = {'test': 'a'}
        a.save(adoc)
        self.server.replicate(aname, bname, callback=self.stop)
        result, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEquals(b[adoc['_id']]['test'], 'a')

        bdoc = b[adoc['_id']]
        bdoc['test'] = 'b'
        b.save([bdoc])
        self.server.replicate(bname, aname, callback=self.stop)
        result, status = self.wait()
        self.assertTrue(status.ok)
        self.assertEquals(a[adoc['_id']]['test'], 'b')
        self.assertEquals(b[adoc['_id']]['test'], 'b')

    def test_replicate_continuous(self):
        aname, a = self.temp_db()
        bname, b = self.temp_db()
        self.server.replicate(aname, bname, continuous=True, callback=self.stop)
        result, status = self.wait()
        self.assertTrue(status.ok)
        self.assertTrue(result['ok'])
        version = tuple(int(i) for i in self.server.version().split('.')[:2])
        if version >= (0, 10):
            self.assertTrue('_local_id' in result)

    def test_uuids(self):
        self.server.uuids(callback=self.stop)
        uus, status = self.wait()
        self.assertEquals(list, type(uus))

        self.server.uuids(count=10, callback=self.stop)
        uus, status = self.wait()
        assert type(uus) == list and len(uus) == 10


class AsyncDatabaseTestCase(AsyncTestCase):

    def test_save_new(self):
        doc = {'foo': 'bar'}
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(doc['_id'] is not None)
        self.assertTrue(doc['_rev'] is not None)
        self.assertTrue(doc['_id'] not in conflicts)

        self.db.get(doc['_id'], callback=self.stop)
        same_doc, status = self.wait()
        self.assertEqual(same_doc['foo'], 'bar')

    def test_save_new_with_id(self):
        doc = {'_id': 'foo'}
        self.db.save(doc, callback=self.stop)
        conflicts, _ = self.wait()
        self.assertEqual(0, len(conflicts.pending))
        same_doc = self.db[doc['_id']]
        self.assertTrue(doc['_id'] == same_doc['_id'] == 'foo')
        self.assertEqual(doc['_rev'], same_doc['_rev'])

    def test_save_existing(self):
        doc = {}
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        self.assertEqual(0, len(conflicts.pending))
        
        id_rev_old = doc['_rev']
        doc['foo'] = True
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        self.assertEqual(0, len(conflicts.pending))

        id_rev_new = doc['_rev']
        self.assertTrue(id_rev_old != id_rev_new)

    def test_save_new_batch(self):
        doc = {'_id': 'foo'}
        self.db.save(doc, batch='ok', callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(doc['_id'] not in conflicts)
        self.assertTrue('_rev' not in doc)
        
    def test_save_existing_batch(self):
        doc = {'_id': 'foo'}
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        
        id_rev_inital = doc['_rev']
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        id_rev_old = doc['_rev']
        self.db.save(doc, batch='ok', callback=self.stop)
        conflicts, status = self.wait()
        id_rev_new = doc['_rev']
        
        self.assertNotEquals(id_rev_inital, id_rev_old)
        self.assertEqual(id_rev_old, doc['_rev'])

    def test_exists(self):
        self.db.exists(callback=self.stop)
        exists, status = self.wait()
        self.assertTrue(exists)

        nexistepas = Database('c_o_r_d_u_r_o_y/missing')
        nexistepas.exists(callback=self.stop)
        exists, status = self.wait()
        self.assertFalse(exists)

    def test_commit(self):
        self.db.commit(callback=self.stop)
        result, status = self.wait()
        self.assertTrue(result['ok'])

    def test_create_large_doc(self):
        doc = {'_id':'foo', 'data': '0123456789' * 110 * 1024} # 10 MB
        self.db.save(doc, callback=self.stop)
        conflicts, status = self.wait()
        
        self.db.get('foo', callback=self.stop)
        redoc, status = self.wait()

        self.assertEqual('foo', redoc['_id'])
        self.assertEqual(doc['data'], redoc['data'])

    def test_doc_id_quoting(self):
        orig = {'_id':'foo/bar','foo': 'bar'}
        self.db.save(orig, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(status.ok)
        
        self.db.get('foo/bar', callback=self.stop)
        doc,status = self.wait()
        self.assertEqual('bar', doc['foo'])
        
        self.db.delete(doc, callback=self.stop)
        resp, status = self.wait()
        self.assertTrue(status.ok)
        
        self.db.get('foo/bar', callback=self.stop)
        nexistepas, status = self.wait()
        self.assertTrue(status.error is NotFound)
        self.assertIsNone(nexistepas)

    def test_unicode(self):
        orig = {'_id':u'føø', u'bår': u'Iñtërnâtiônàlizætiøn', 'baz': 'ASCII'}
        self.db.save(orig, callback=self.stop)
        _, status = self.wait()
        
        self.db.get(u'føø', callback=self.stop)
        doc, status = self.wait()
        self.assertEqual(u'Iñtërnâtiônàlizætiøn', doc[u'bår'])
        self.assertEqual(u'ASCII', doc[u'baz'])

    def test_disallow_nan(self):
        doc = {'_id':'foo', u'number': float('nan')}
        self.assertRaises(ValueError, self.db.save, doc, callback=self.stop)

    def test_disallow_none_id(self):
        deldoc = {'_id': None, '_rev': None}
        self.assertRaises(ValueError, self.db.delete, deldoc, callback=self.stop)

    def test_doc_revs(self):
        doc = {'bar': 42}
        self.db['foo'] = doc
        old_rev = doc['_rev']
        doc['bar'] = 43
        self.db['foo'] = doc
        new_rev = doc['_rev']

        new_doc = self.db.get('foo')
        self.assertEqual(new_rev, new_doc['_rev'])
        new_doc = self.db.get('foo', rev=new_rev)
        self.assertEqual(new_rev, new_doc['_rev'])
        old_doc = self.db.get('foo', rev=old_rev)
        self.assertEqual(old_rev, old_doc['_rev'])

        self.db.revisions('foo', callback=self.stop)
        revs, status = self.wait()
        self.assertEqual(revs[0], new_rev)
        self.assertEqual(revs[1], old_rev)
        # gen = self.db.revisions('crap')
        # self.assertRaises(StopIteration, lambda: gen.next())

        self.db.compact(callback=self.stop)
        successful, status = self.wait()
        self.assertTrue(successful)
        while True:
            self.db.info(callback=self.stop)
            info, status = self.wait()
            if not info['compact_running']:
                break
            time.sleep(0.2)

        self.db.get('foo', rev=old_rev, callback=self.stop)
        doc, status = self.wait()
        self.assertTrue(status.error is NotFound)

    def test_attachment_crud(self):
        doc = {'bar': 42}
        self.db['foo'] = doc
        old_rev = doc['_rev']
    
        self.db.put_attachment(doc, 'Foo bar', 'foo.txt', 'text/plain', callback=self.stop)
        resp, status = self.wait()
        self.assertNotEquals(old_rev, doc['_rev'])
        self.assertTrue(doc['_attachments']['foo.txt']['added'])
    
        self.db.get('foo', callback=self.stop)
        doc, status = self.wait()
        
        attachment = doc['_attachments']['foo.txt']
        self.assertEqual(len('Foo bar'), attachment['length'])
        self.assertEqual('text/plain', attachment['content_type'])
    
        self.db.get_attachment(doc, 'foo.txt', callback=self.stop)
        attfile, status = self.wait()
        self.assertEqual('Foo bar',attfile)

        self.db.get_attachment('foo', 'foo.txt', callback=self.stop)
        attfile, status = self.wait()
        self.assertEqual('Foo bar', attfile)
                             
        old_rev = doc['_rev']
        self.db.delete_attachment(doc, 'foo.txt', callback=self.stop)
        response, status = self.wait()
        self.assertNotEquals(old_rev, doc['_rev'])
        
        new_doc = self.db['foo']
        self.assertEqual(None, new_doc.get('_attachments'))
    
    def test_attachment_crud_with_files(self):
        doc = {'bar': 42}
        self.db['foo'] = doc
        old_rev = doc['_rev']
        fileobj = StringIO('Foo bar baz')
    
        self.db.put_attachment(doc, fileobj, 'foo.txt', callback=self.stop)
        resp, status = self.wait()
        self.assertNotEquals(old_rev, doc['_rev'])
        self.assertTrue(doc['_attachments']['foo.txt']['added'])
    
        doc = self.db['foo']
        attachment = doc['_attachments']['foo.txt']
        self.assertEqual(len('Foo bar baz'), attachment['length'])
        self.assertEqual('text/plain', attachment['content_type'])
    
        self.db.get_attachment(doc, 'foo.txt', callback=self.stop)
        attfile, status = self.wait()
        self.assertEqual('Foo bar baz',attfile)

        self.db.get_attachment('foo', 'foo.txt', callback=self.stop)
        attfile, status = self.wait()
        self.assertEqual('Foo bar baz', attfile)
    
        old_rev = doc['_rev']
        self.db.delete_attachment(doc, 'foo.txt', callback=self.stop)
        response, status = self.wait()
        self.assertNotEquals(old_rev, doc['_rev'])
        
        new_doc = self.db['foo']
        self.assertEqual(None, new_doc.get('_attachments'))

    
    def test_empty_attachment(self):
        doc = {}
        self.db['foo'] = doc
        old_rev = doc['_rev']
    
        self.db.put_attachment(doc, '', 'empty.txt', callback=self.stop)
        resp, status = self.wait()
        self.assertNotEquals(old_rev, doc['_rev'])
        self.assertTrue(doc['_attachments']['empty.txt']['added'])
    
        doc = self.db['foo']
        attachment = doc['_attachments']['empty.txt']
        self.assertEqual(0, attachment['length'])
    
    def test_missing_attachment(self):
        doc = {}
        self.db['foo'] = doc
        self.assertRaises(NotFound, self.db.get_attachment, doc, 'missing.txt')
        self.db.get_attachment(doc, 'missing.txt', callback=self.stop)
        resp, status = self.wait()
        self.assertTrue(status.error is NotFound)
    
    def test_attachment_from_fs(self):
        tmpdir = tempfile.mkdtemp()
        tmpfile = os.path.join(tmpdir, 'test.txt')
        f = open(tmpfile, 'w')
        f.write('Hello!')
        f.close()
        doc = {}
        
        self.db['foo'] = doc
        self.db.put_attachment(doc, open(tmpfile), callback=self.stop)
        resp, status = self.wait()
        self.assertTrue(doc['_attachments']['test.txt']['added'])
        self.assertTrue(doc['_attachments']['test.txt']['content_type'] == 'text/plain')
        
        doc = self.db.get('foo')
        self.assertTrue(doc['_attachments']['test.txt']['content_type'] == 'text/plain')
        shutil.rmtree(tmpdir)
    
    def test_attachment_no_filename(self):
        doc = {}
        self.db['foo'] = doc
        self.assertRaises(ValueError, self.db.put_attachment, doc, '', callback=self.stop)
        
    def test_json_attachment(self):
        doc = {}
        self.db['foo'] = doc
        self.db.put_attachment(doc, '{}', 'test.json', 'application/json', callback=self.stop)
        _, status = self.wait()
        
        self.db.get_attachment(doc, 'test.json', callback=self.stop)
        jsonatt, status = self.wait()
        self.assertEquals(jsonatt, '{}')

    def test_include_docs(self):
        doc = {'foo': 42, 'bar': 40}
        self.db['foo'] = doc

        self.db.query('function(doc) { emit(doc._id, null); }', include_docs=True, callback=self.stop)
        rows, status = self.wait()
        self.assertEqual(1, len(rows))
        self.assertEqual(doc, rows[0].doc)

    def test_query_multi_get(self):
        for i in range(1, 6):
            self.db.save({'i': i})
        self.db.query('function(doc) { emit(doc.i, null); }', keys=range(1, 6, 2), callback=self.stop)
        rows, status = self.wait()
        
        self.assertEqual(3, len(rows))
        for idx, i in enumerate(range(1, 6, 2)):
            self.assertEqual(i, rows[idx].key)

    def test_bulk_update_conflict(self):
        docs = [
            dict(type='Person', name='John Doe'),
            dict(type='Person', name='Mary Jane'),
            dict(type='City', name='Gotham City')
        ]
        self.db.save(docs, callback=self.stop)
        conflicts, status = self.wait()
        self.assertEqual(0, len(conflicts.pending))

        # update the first doc to provoke a conflict in the next bulk update
        doc = docs[0].copy()
        self.db[doc['_id']] = doc

        self.db.save(docs, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(status.ok) # no 409 w/ bulk_docs even when there are conflicts        
        self.assertTrue(docs[1]['_id'] in conflicts.resolved)
        
        ctx = conflicts.pending[doc['_id']]
        self.assertEqual(ctx.error, 'conflict')
        self.assertEqual(ctx.doc, Document(docs[0]))

    def test_bulk_update_all_or_nothing(self):
        docs = [
            dict(type='Person', name='John Doe'),
            dict(type='Person', name='Mary Jane'),
            dict(type='City', name='Gotham City')
        ]
        self.db.save(docs)

        # update the first doc to provoke a conflict in the next bulk update
        doc = docs[0].copy()
        doc['name'] = 'Jane Doe'
        self.db[doc['_id']] = doc

        self.db.save(docs, all_or_nothing=True, callback=self.stop)
        conflicts, status = self.wait()
        self.assertTrue(doc['_id'] not in conflicts)

        self.db.get(doc['_id'], conflicts=True, callback=self.stop)
        doc, status = self.wait()
        assert '_conflicts' in doc
        self.db.get(doc['_id'], open_revs='all', callback=self.stop)
        revs, status = self.wait()
        assert len(revs) == 2

    def test_bulk_update_bad_doc(self):
        self.assertRaises(TypeError, self.db.save, docs=[object()], callback=self.stop)

    def test_copy_doc(self):
        self.db['foo'] = {'status': 'testing'}
        self.db.copy('foo', 'bar', callback=self.stop)
        result, status = self.wait()
        self.assertEqual(result, self.db['bar']._rev)

    def test_copy_doc_conflict(self):
        self.db['bar'] = {'status': 'idle'}
        self.db['foo'] = {'status': 'testing'}
        
        self.db.copy('foo','bar', callback=self.stop)
        result, status = self.wait()
        self.assertTrue(status.error is Conflict)

    def test_copy_doc_overwrite(self):
        self.db['bar'] = {'status': 'idle'}
        self.db['foo'] = {'status': 'testing'}
        
        bdoc = self.db['bar']
        self.db.copy('foo', bdoc, callback=self.stop)
        rev, status = self.wait()
        self.assertTrue(status.ok)

        doc = self.db['bar']
        self.assertEqual(rev, doc._rev)
        self.assertEqual('testing', doc['status'])

    def test_copy_doc_srcobj(self):
        self.db['foo'] = {'status': 'testing'}
        self.db.copy(self.db['foo'], 'bar', callback=self.stop)
        rev, status = self.wait()
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_destobj_norev(self):
        self.db['foo'] = {'status': 'testing'}
        self.db.copy('foo', {'_id': 'bar'}, callback=self.stop)
        rev, status = self.wait()
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_src_dictlike(self):
        class DictLike(object):
            def __init__(self, doc):
                self.doc = doc
            def items(self):
                return self.doc.items()
        self.db['foo'] = {'status': 'testing'}
        self.db.copy(DictLike(self.db['foo']), 'bar', callback=self.stop)
        rev, status = self.wait()
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_dest_dictlike(self):
        class DictLike(object):
            def __init__(self, doc):
                self.doc = doc
            def items(self):
                return self.doc.items()
        self.db['foo'] = {'status': 'testing'}
        self.db['bar'] = {}
        self.db.copy('foo', DictLike(self.db['bar']), callback=self.stop)
        rev, status = self.wait()
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_src_baddoc(self):
        self.assertRaises(TypeError, self.db.copy, object(), 'bar', callback=self.stop)

    def test_copy_doc_dest_baddoc(self):
        self.assertRaises(TypeError, self.db.copy, 'foo', object(), callback=self.stop)

    def test_changes(self):
        self.db['foo'] = {'bar': True}
        self.db.changes(since=0, callback=self.stop)
        changes, status = self.wait()
        self.assertEqual(changes['last_seq'], 1)
        first = changes['results'][0]
        self.assertEqual(first['seq'], 1)
        self.assertEqual(first['id'], 'foo')
    
    def test_changes_feed(self):
        listener = ChangesListener(self.db)
        
        half = 30
        for i in xrange(half):
            self.db.save({'n':i})
        listener.report(self.stop)
        changes = self.wait()[0]
        self.assertTrue(max(changes.keys())==half)
        
        for i in xrange(half):
            self.db.save({'n':half+i})        
        listener.report(self.stop)
        changes = self.wait()[0]
        self.assertTrue(max(changes.keys())==half*2)
        
        last_change = changes[max(changes.keys())][-1]
        self.assertEqual(last_change['seq'], half*2)
        listener._feed.stop()

    def test_purge(self):
        doc = {'a': 'b'}
        self.db['foo'] = doc
        self.db.purge([doc], callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp['purge_seq'], 1)

    def test_json_encoding_error(self):
        doc = {'now': datetime.now()}
        self.assertRaises(TypeError, self.db.save, doc, callback=self.stop)


class AsyncViewTestCase(AsyncTestCase):

    def test_row_object(self):
        
        self.db.view('_all_docs', keys=['blah'], callback=self.stop)
        rows, status = self.wait()

        row = rows[0]
        self.assertEqual(repr(row), u"<Row \"blah\" / None «not_found»>".encode('utf-8'))
        self.assertEqual(row.id, None)
        self.assertEqual(row.key, 'blah')
        self.assertEqual(row.value, None)
        self.assertEqual(row.error, 'not_found')

        self.db.save({'_id': 'xyz', 'foo': 'bar'}, callback=self.stop)
        conflicts, status = self.wait()        
        self.db.view('_all_docs', keys=['xyz'], callback=self.stop)
        rows, status = self.wait()

        row = rows[0]
        self.assertEqual(row.id, 'xyz')
        self.assertEqual(row.key, 'xyz')
        self.assertEqual(row.value.keys(), ['rev'])
        self.assertEqual(row.error, None)

    def test_view_multi_get(self):
        for i in range(1, 6):
            self.db.save({'i': i})
        self.db['_design/test'] = {
            'language': 'javascript',
            'views': {
                'multi_key': {'map': 'function(doc) { emit(doc.i, null); }'}
            }
        }

        self.db.view('test/multi_key', keys=range(1, 6, 2), callback=self.stop)
        rows, status = self.wait()

        self.assertEqual(3, len(rows))
        for idx, i in enumerate(range(1, 6, 2)):
            self.assertEqual(i, rows[idx].key)

    def test_ddoc_info(self):
        self.db['_design/test'] = {
            'language': 'javascript',
            'views': {
                'test': {'map': 'function(doc) { emit(doc.type, null); }'}
            }
        }
        self.db.info('test', callback=self.stop)
        info, status = self.wait()
        self.assertEqual(info['view_index']['compact_running'], False)

    def test_view_compaction(self):
        for i in range(1, 6):
            self.db.save({'i': i})
        self.db['_design/test'] = {
            'language': 'javascript',
            'views': {
                'multi_key': {'map': 'function(doc) { emit(doc.i, null); }'}
            }
        }

        self.db.view('test/multi_key')
        self.db.compact('test', callback=self.stop)
        result, status = self.wait()
        self.assertTrue(result)

    def test_view_cleanup(self):

        for i in range(1, 6):
            self.db.save({'i': i})

        self.db['_design/test'] = {
            'language': 'javascript',
            'views': {
                'multi_key': {'map': 'function(doc) { emit(doc.i, null); }'}
            }
        }
        self.db.view('test/multi_key')

        ddoc = self.db['_design/test']
        ddoc['views'] = {
            'ids': {'map': 'function(doc) { emit(doc._id, null); }'}
        }
        self.db.save([ddoc])
        self.db.view('test/ids')
        self.db.cleanup(callback=self.stop)
        result, status = self.wait()
        self.assertTrue(result)

    def test_init_with_resource(self):
        self.db['foo'] = {}
        self.db.view('_all_docs', callback=self.stop)
        rows, status = self.wait()
        self.assertEquals(len(rows), 1)

    def test_iter_view(self):
        self.db['foo'] = {}
        self.db.view('_all_docs', callback=self.stop)
        rows, status = self.wait()
        
        count = 0
        for r in rows: count += 1
        self.assertEquals(count, 1)

    def test_tmpview_repr(self):
        mapfunc = "function(doc) {emit(null, null);}"
        self.db.query(mapfunc, callback=self.stop)
        rows, status = self.wait()
        self.assertTrue('_temp_view' in repr(rows))
        # self.assertTrue(mapfunc in repr(view))

    # def test_wrapper_iter(self):
    #     class Wrapper(object):
    #         def __init__(self, doc):
    #             pass
    #     self.db['foo'] = {}
    #     self.assertTrue(isinstance(list(self.db.view('_all_docs', wrapper=Wrapper))[0], Wrapper))
    # 
    # def test_wrapper_rows(self):
    #     class Wrapper(object):
    #         def __init__(self, doc):
    #             pass
    #     self.db['foo'] = {}
    #     self.assertTrue(isinstance(self.db.view('_all_docs', wrapper=Wrapper).rows[0], Wrapper))

    def test_properties(self):
        self.db.view('_all_docs', callback=self.stop)
        view, status = self.wait()
        for attr in ['rows', 'total_rows', 'offset']:
            self.assertTrue(getattr(view, attr) is not None)

class AsyncShowListTestCase(AsyncTestCase):

    show_func = """
        function(doc, req) {
            return {"body": req.id + ":" + (req.query.r || "<default>")};
        }
        """

    list_func = """
        function(head, req) {
            start({headers: {'Content-Type': 'text/csv'}});
            if (req.query.include_header) {
                send('id' + '\\r\\n');
            }
            var row;
            while (row = getRow()) {
                send(row.id + '\\r\\n');
            }
        }
        """

    design_doc = {'_id': '_design/foo',
                  'shows': {'bar': show_func},
                  'views': {'by_id': {'map': "function(doc) {emit(doc._id, null)}"},
                            'by_name': {'map': "function(doc) {emit(doc.name, null)}"}},
                  'lists': {'list': list_func}}

    def setUp(self):
        super(AsyncShowListTestCase, self).setUp()
        # Workaround for possible bug in CouchDB. Adding a timestamp avoids a
        # 409 Conflict error when pushing the same design doc that existed in a
        # now deleted database.
        design_doc = dict(self.design_doc)
        design_doc['timestamp'] = time.time()
        self.db.save(design_doc)
        self.db.save([{'_id': '1', 'name': 'one'}, {'_id': '2', 'name': 'two'}])

    def test_show_urls(self):
        self.db.show('_design/foo/_show/bar', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'null:<default>')
        
        self.db.show('foo/bar', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'null:<default>')

    def test_show_docid(self):
        self.db.show('foo/bar', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'null:<default>')

        self.db.show('foo/bar', id='1', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'1:<default>')

        self.db.show('foo/bar', id='2', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'2:<default>')

    def test_show_params(self):
        self.db.show('foo/bar', r='abc', callback=self.stop)
        txt, status = self.wait()
        self.assertEqual(txt, u'null:abc')

    def test_list(self):
        self.db.list('foo/list', 'foo/by_id', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp.body, u'1\r\n2\r\n')

        self.db.list('foo/list', 'foo/by_id', include_header='true', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp.body, u'id\r\n1\r\n2\r\n')

    def test_list_keys(self):
        self.db.list('foo/list', 'foo/by_id', keys=['1'], callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp.body, u'1\r\n')

    def test_list_view_params(self):
        self.db.list('foo/list', 'foo/by_name', startkey='o', endkey='p', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp.body, u'1\r\n')

        self.db.list('foo/list', 'foo/by_name', descending=True, callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp.body, u'2\r\n1\r\n')

class AsyncUpdateHandlerTestCase(AsyncTestCase):
    update_func = """
        function(doc, req) {
          if (!doc) {
            if (req.id) {
              return [{_id : req.id}, "new doc"]
            }
            return [null, "empty doc"];
          }
          doc.name = "hello";
          return [doc, "hello doc"];
        }
    """

    design_doc = {'_id': '_design/foo',
                  'language': 'javascript',
                  'updates': {'bar': update_func}}

    def setUp(self):
        super(AsyncUpdateHandlerTestCase, self).setUp()
        # Workaround for possible bug in CouchDB. Adding a timestamp avoids a
        # 409 Conflict error when pushing the same design doc that existed in a
        # now deleted database.
        design_doc = dict(self.design_doc)
        design_doc['timestamp'] = time.time()
        self.db.save(design_doc)
        self.db.save([{'_id': 'existed', 'name': 'bar'}])

    def test_empty_doc(self):
        # self.db.save()
        self.db.update('foo/bar', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp, 'empty doc')        

    def test_new_doc(self):
        self.db.update('foo/bar', 'new', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp, 'new doc')        

    def test_update_doc(self):
        self.db.update('foo/bar', 'existed', callback=self.stop)
        resp, status = self.wait()
        self.assertEqual(resp, 'hello doc')        

class ChangesListener(object):    
    def __init__(self, db):
        self._feed = db.changes(feed='continuous', latency=0, callback=self.got_changes)
        self.changes = {}

    def got_changes(self, seq, changes):
        self.changes[seq] = changes
        
    def report(self, callback):
        import tornado
        tornado.ioloop.IOLoop.instance().add_timeout(timedelta(seconds=1.5), lambda: self.done(callback))

    def done(self, callback):
        callback(self.changes)

tornado = None        
def suite():
    suite = unittest.TestSuite()
    try:
        import tornado        
        suite.addTest(unittest.makeSuite(AsyncCouchTests, 'test'))
        suite.addTest(unittest.makeSuite(AsyncCouchTestCase, 'test'))
        suite.addTest(unittest.makeSuite(AsyncDatabaseTestCase, 'test'))
        suite.addTest(unittest.makeSuite(AsyncViewTestCase, 'test'))
        suite.addTest(unittest.makeSuite(AsyncShowListTestCase, 'test'))
        suite.addTest(unittest.makeSuite(AsyncUpdateHandlerTestCase, 'test'))
    except ImportError:
        print "Running blocking tests only..."
        
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')











