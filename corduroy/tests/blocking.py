#!/usr/bin/env python
# encoding: utf-8
"""
corduroy.tests.blocking
"""

from datetime import datetime
import doctest
import os
import os.path
import shutil
from StringIO import StringIO
import time
import tempfile
import threading
import unittest
import urlparse
from pdb import set_trace as tron

import testutil
import corduroy.io as io
from corduroy.atoms import *
from corduroy.exceptions import *
from corduroy.couchdb import *

# all tests adopted/adapted from couchdb-python
class CouchTests(testutil.TempDatabaseMixin, unittest.TestCase):

    def _create_test_docs(self, num):
        for i in range(num):
            self.db[str(i)] = {'a': i + 1, 'b': (i + 1) ** 2}

    def test_basics(self):
        self.assertEqual(0, len(self.db))

        # create a document
        data = {'a': 1, 'b': 1}
        self.db['0'] = data
        self.assertEqual('0', data['_id'])
        assert '_rev' in data
        doc = self.db['0']
        self.assertEqual('0', doc['_id'])
        self.assertEqual(data['_rev'], doc['_rev'])
        self.assertEqual(1, len(self.db))

        # delete a document
        del self.db['0']
        self.assertRaises(NotFound, self.db.__getitem__, '0')

        # test _all_docs
        self._create_test_docs(4)
        self.assertEqual(4, len(self.db))
        for doc_id in self.db:
            assert int(doc_id) in range(4)

        # test a simple query
        query = """function(doc) {
            if (doc.a==4)
                emit(null, doc.b);
        }"""
        result = list(self.db.query(query))
        self.assertEqual(1, len(result))
        self.assertEqual('3', result[0].id)
        self.assertEqual(16, result[0].value)

        # modify a document, and redo the query
        doc = self.db['0']
        doc['a'] = 4
        self.db['0'] = doc
        result = list(self.db.query(query))
        self.assertEqual(2, len(result))

        # add more documents, and redo the query again
        self.db.save({'a': 3, 'b': 9})
        self.db.save({'a': 4, 'b': 16})
        result = list(self.db.query(query))
        self.assertEqual(3, len(result))
        self.assertEqual(6, len(self.db))

        # delete a document, and redo the query once more
        del self.db['0']
        result = list(self.db.query(query))
        self.assertEqual(2, len(result))
        self.assertEqual(5, len(self.db))

    def test_conflict_detection(self):
        doc1 = {'a': 1, 'b': 1}
        self.db['foo'] = doc1
        doc2 = self.db['foo']
        self.assertEqual(doc1['_id'], doc2['_id'])
        self.assertEqual(doc1['_rev'], doc2['_rev'])

        # make conflicting modifications
        doc1['a'] = 2
        doc2['a'] = 3
        self.db['foo'] = doc1
        self.assertRaises(Conflict, self.db.__setitem__, 'foo', doc2)

        # try submitting without the revision info
        data = {'_id': 'foo', 'a': 3, 'b': 1}
        self.assertRaises(Conflict, self.db.__setitem__, 'foo', data)

        del self.db['foo']
        self.db['foo'] = data

    def test_lots_of_docs(self):
        num = 100 # Crank up manually to really test
        for i in range(num): 
            self.db[str(i)] = {'integer': i, 'string': str(i)}
        self.assertEqual(num, len(self.db))

        query = """function(doc) {
            emit(doc.integer, null);
        }"""
        results = list(self.db.query(query))
        self.assertEqual(num, len(results))
        for idx, row in enumerate(results):
            self.assertEqual(idx, row.key)

        results = list(self.db.query(query, descending=True))
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
        results = list(self.db.query(query))
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
        results = list(self.db.query(query))
        self.assertEqual(12, len(results))
        self.assertEqual("Wilmington, NC", results[10].key)

        # Remove a document and redo the query again
        del self.db['MA']
        results = list(self.db.query(query))
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
        self.db.save({'longtext': longtext})
        self.db.save({'longtext': longtext})
        self.db.save({'longtext': longtext})
        self.db.save({'longtext': longtext})

        query = """function(doc) {
            emit(null, doc.longtext);
        }"""
        results = list(self.db.query(query))
        self.assertEqual(4, len(results))

    def test_utf8_encoding(self):
        texts = [
            u"1. Ascii: hello",
            u"2. Russian: На берегу пустынных волн",
            u"3. Math: ∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i),",
            u"4. Geek: STARGΛ̊TE SG-1",
            u"5. Braille: ⡌⠁⠧⠑ ⠼⠁⠒  ⡍⠜⠇⠑⠹⠰⠎ ⡣⠕⠌"
        ]
        for idx, text in enumerate(texts):
            self.db[str(idx)] = {'text': text}
        for idx, text in enumerate(texts):
            doc = self.db[str(idx)]
            self.assertEqual(text, doc['text'])

        query = """function(doc) {
            emit(doc.text, null);
        }"""
        for idx, row in enumerate(self.db.query(query)):
            self.assertEqual(texts[idx], row.key)

    def test_design_docs(self):
        for i in range(50): 
            self.db[str(i)] = {'integer': i, 'string': str(i)}
        self.db['_design/test'] = {'views': {
            'all_docs': {'map': 'function(doc) { emit(doc.integer, null) }'},
            'no_docs': {'map': 'function(doc) {}'},
            'single_doc': {'map': 'function(doc) { if (doc._id == "1") emit(null, 1) }'}
        }}
        for idx, row in enumerate(self.db.view('test/all_docs')):
            self.assertEqual(idx, row.key)
        self.assertEqual(0, len(list(self.db.view('test/no_docs'))))
        self.assertEqual(1, len(list(self.db.view('test/single_doc'))))

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
        rows = iter(self.db.query(query))
        self.assertEqual(None, rows.next().value)
        for idx, row in enumerate(rows):
            self.assertEqual(values[idx + 1], row.key)

        rows = self.db.query(query, descending=True)
        for idx, row in enumerate(rows):
            if idx < len(values):
                self.assertEqual(values[len(values) - 1- idx], row.key)
            else:
                self.assertEqual(None, row.value)

        for value in values:
            rows = list(self.db.query(query, key=value))
            self.assertEqual(1, len(rows))
            self.assertEqual(value, rows[0].key)

class CouchTestCase(testutil.TempDatabaseMixin, unittest.TestCase):

    def test_init_with_resource(self):
        # sess = io.Session()
        res = io.Resource('http://127.0.0.1:5984')
        serv = Couch(url=res)
        serv.config()

    # def test_init_with_session(self):
    #     # sess = io.Session()
    #     serv = Couch(DEFAULT_BASE_URL)
    #     serv.config()
    #     self.assertTrue(serv.resource.session is sess)

    def test_exists(self):
        self.assertTrue(Couch())
        self.assertFalse(Couch('http://localhost:9999'))

    def test_repr(self):
        repr(self.server)

    def test_server_vars(self):
        version = self.server.version()
        self.assertTrue(isinstance(version, basestring))
        config = self.server.config()
        self.assertTrue(isinstance(config, dict))
        tasks = self.server.tasks()
        self.assertTrue(isinstance(tasks, list))

    def test_server_stats(self):
        stats = self.server.stats()
        self.assertTrue(isinstance(stats, dict))
        stats = self.server.stats('httpd/requests')
        self.assertTrue(isinstance(stats, dict))
        self.assertTrue(len(stats) == 1 and len(stats['httpd']) == 1)

    def test_get_db_missing(self):
        self.assertRaises(io.NotFound,
                          lambda: self.server['c_o_r_d_u_r_o_y/missing'])

    def test_create_db_conflict(self):
        name, db = self.temp_db()
        self.assertRaises(io.PreconditionFailed, self.server.create,
                          name)

    def test_delete_db(self):
        name, db = self.temp_db()
        assert name in self.server
        self.del_db(name)
        assert name not in self.server

    def test_delete_db_missing(self):
        self.assertRaises(io.NotFound, self.server.delete,
                          'c_o_r_d_u_r_o_y/missing')

    def test_replicate(self):
        aname, a = self.temp_db()
        bname, b = self.temp_db()
        adoc = {'test': 'a'}
        a.save(adoc)
        result = self.server.replicate(aname, bname)
        self.assertEquals(result['ok'], True)
        self.assertEquals(b[adoc['_id']]['test'], 'a')

        bdoc = b[adoc['_id']]
        bdoc['test'] = 'b'
        b.save([bdoc])
        self.server.replicate(bname, aname)
        self.assertEquals(a[adoc['_id']]['test'], 'b')
        self.assertEquals(b[adoc['_id']]['test'], 'b')

    # def test_replicate_continuous(self):
    #     # aname, a = self.temp_db()
    #     # bname, b = self.temp_db()
    #     # result = self.server.replicate(aname, bname, continuous=True)
    #     # self.assertEquals(result['ok'], True)
    #     # version = tuple(int(i) for i in self.server.version().split('.')[:2])
    #     # if version >= (0, 10):
    #     #     self.assertTrue('_local_id' in result)

    def test_iter(self):
        aname, a = self.temp_db()
        bname, b = self.temp_db()
        dbs = list(self.server)
        self.assertTrue(aname in dbs)
        self.assertTrue(bname in dbs)

    def test_len(self):
        self.temp_db()
        self.temp_db()
        self.assertTrue(len(self.server) >= 2)

    def test_uuids(self):
        ls = self.server.uuids()
        assert type(ls) == list
        ls = self.server.uuids(count=10)
        assert type(ls) == list and len(ls) == 10


class DatabaseTestCase(testutil.TempDatabaseMixin, unittest.TestCase):

    def test_save_new(self):
        doc = {'foo': 'bar'}
        conflicts = self.db.save(doc)
        self.assertTrue(doc['_id'] is not None)
        self.assertTrue(doc['_rev'] is not None)
        self.assertTrue(doc['_id'] not in conflicts)

        same_doc = self.db.get(doc['_id'])
        self.assertEqual(same_doc['foo'], 'bar')

    def test_save_new_with_id(self):
        doc = {'_id': 'foo'}
        self.db.save(doc)
        same_doc = self.db[doc['_id']]
        self.assertTrue(doc['_id'] == same_doc['_id'] == 'foo')
        self.assertEqual(doc['_rev'], same_doc['_rev'])

    def test_save_existing(self):
        doc = {}
        self.db.save(doc)
        id_rev_old = doc['_rev']
        doc['foo'] = True
        self.db.save(doc)
        id_rev_new = doc['_rev']
        self.assertTrue(id_rev_old != id_rev_new)

    def test_save_new_batch(self):
        doc = {'_id': 'foo'}
        conflicts = self.db.save(doc, batch='ok')        
        self.assertTrue(doc['_id'] not in conflicts)
        self.assertTrue('_rev' not in doc)

    def test_save_existing_batch(self):
        doc = {'_id': 'foo'}
        self.db.save(doc)
        id_rev_inital = doc['_rev']
        self.db.save(doc)
        id_rev_old = doc['_rev']
        self.db.save(doc, batch='ok')
        id_rev_new = doc['_rev']
        self.assertNotEquals(id_rev_inital, id_rev_old)
        self.assertEqual(id_rev_old, doc['_rev'])

    def test_exists(self):
        self.assertTrue(self.db)
        self.assertFalse(Database('c_o_r_d_u_r_o_y/missing'))

    def test_name(self):
        # Access name assigned during creation.
        name, db = self.temp_db()
        self.assertTrue(db.name == name)
        # Access lazily loaded name,
        self.assertTrue(Database(db.resource.url).name == name)

    def test_commit(self):
        self.assertTrue(self.db.commit()['ok'] == True)

    def test_create_large_doc(self):
        self.db['foo'] = {'data': '0123456789' * 110 * 1024} # 10 MB
        self.assertEqual('foo', self.db['foo']['_id'])

    def test_doc_id_quoting(self):
        self.db['foo/bar'] = {'foo': 'bar'}
        self.assertEqual('bar', self.db['foo/bar']['foo'])
        del self.db['foo/bar']
        self.assertRaises(NotFound, self.db.get, 'foo/bar')

    def test_unicode(self):
        self.db[u'føø'] = {u'bår': u'Iñtërnâtiônàlizætiøn', 'baz': 'ASCII'}
        self.assertEqual(u'Iñtërnâtiônàlizætiøn', self.db[u'føø'][u'bår'])
        self.assertEqual(u'ASCII', self.db[u'føø'][u'baz'])

    def test_disallow_nan(self):
        try:
            self.db['foo'] = {u'number': float('nan')}
            self.fail('Expected ValueError')
        except ValueError:
            pass

    def test_disallow_none_id(self):
        deldoc = lambda: self.db.delete({'_id': None, '_rev': None})
        self.assertRaises(ValueError, deldoc)

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

        revs = [i for i in self.db.revisions('foo')]
        self.assertEqual(revs[0], new_rev)
        self.assertEqual(revs[1], old_rev)
        # gen = self.db.revisions('crap')
        # self.assertRaises(StopIteration, lambda: gen.next())

        self.assertTrue(self.db.compact())
        while self.db.info()['compact_running']:
            pass

        # 0.10 responds with 404, 0.9 responds with 500, same content
        doc = 'fail'
        try:
            doc = self.db.get('foo', rev=old_rev)
        except NotFound:
            doc = None
        assert doc is None

    def test_attachment_crud(self):
        doc = {'bar': 42}
        self.db['foo'] = doc
        old_rev = doc['_rev']
    
        self.db.put_attachment(doc, 'Foo bar', 'foo.txt', 'text/plain')
        self.assertNotEquals(old_rev, doc['_rev'])
    
        doc = self.db['foo']
        attachment = doc['_attachments']['foo.txt']
        self.assertEqual(len('Foo bar'), attachment['length'])
        self.assertEqual('text/plain', attachment['content_type'])
    
        self.assertEqual('Foo bar',
                         self.db.get_attachment(doc, 'foo.txt'))
        self.assertEqual('Foo bar',
                         self.db.get_attachment('foo', 'foo.txt'))
    
        old_rev = doc['_rev']
        self.db.delete_attachment(doc, 'foo.txt')
        self.assertNotEquals(old_rev, doc['_rev'])
        self.assertEqual(None, self.db['foo'].get('_attachments'))
    
    def test_attachment_crud_with_files(self):
        doc = {'bar': 42}
        self.db['foo'] = doc
        old_rev = doc['_rev']
        fileobj = StringIO('Foo bar baz')
    
        self.db.put_attachment(doc, fileobj, 'foo.txt')
        self.assertNotEquals(old_rev, doc['_rev'])
    
        doc = self.db['foo']
        attachment = doc['_attachments']['foo.txt']
        self.assertEqual(len('Foo bar baz'), attachment['length'])
        self.assertEqual('text/plain', attachment['content_type'])
    
        self.assertEqual('Foo bar baz',
                         self.db.get_attachment(doc, 'foo.txt'))
        self.assertEqual('Foo bar baz',
                         self.db.get_attachment('foo', 'foo.txt'))
    
        old_rev = doc['_rev']
        self.db.delete_attachment(doc, 'foo.txt')
        self.assertNotEquals(old_rev, doc['_rev'])
        self.assertEqual(None, self.db['foo'].get('_attachments'))
    
    def test_empty_attachment(self):
        doc = {}
        self.db['foo'] = doc
        old_rev = doc['_rev']
    
        self.db.put_attachment(doc, '', 'empty.txt')
        self.assertNotEquals(old_rev, doc['_rev'])
    
        doc = self.db['foo']
        attachment = doc['_attachments']['empty.txt']
        self.assertEqual(0, attachment['length'])
    
    def test_default_attachment(self):
        doc = {}
        self.db['foo'] = doc
        self.assertRaises(NotFound, self.db.get_attachment, doc, 'missing.txt')
        # self.assertTrue(self.db.get_attachment(doc, 'missing.txt') is None)
        # sentinel = object()
        # self.assertTrue(self.db.get_attachment(doc, 'missing.txt', sentinel) is sentinel)
    
    def test_attachment_from_fs(self):
        tmpdir = tempfile.mkdtemp()
        tmpfile = os.path.join(tmpdir, 'test.txt')
        f = open(tmpfile, 'w')
        f.write('Hello!')
        f.close()
        doc = {}
        self.db['foo'] = doc
        self.db.put_attachment(doc, open(tmpfile))
        doc = self.db.get('foo')
        self.assertTrue(doc['_attachments']['test.txt']['content_type'] == 'text/plain')
        shutil.rmtree(tmpdir)
    
    def test_attachment_no_filename(self):
        doc = {}
        self.db['foo'] = doc
        self.assertRaises(ValueError, self.db.put_attachment, doc, '')
    
    def test_json_attachment(self):
        doc = {}
        self.db['foo'] = doc
        self.db.put_attachment(doc, '{}', 'test.json', 'application/json')
        self.assertEquals(self.db.get_attachment(doc, 'test.json'), '{}')

    def test_include_docs(self):
        doc = {'foo': 42, 'bar': 40}
        self.db['foo'] = doc

        rows = list(self.db.query(
            'function(doc) { emit(doc._id, null); }',
            include_docs=True
        ))
        self.assertEqual(1, len(rows))
        self.assertEqual(doc, rows[0].doc)

    def test_query_multi_get(self):
        for i in range(1, 6):
            self.db.save({'i': i})
        res = list(self.db.query('function(doc) { emit(doc.i, null); }',
                                 keys=range(1, 6, 2)))
        self.assertEqual(3, len(res))
        for idx, i in enumerate(range(1, 6, 2)):
            self.assertEqual(i, res[idx].key)

    def test_bulk_update_conflict(self):
        docs = [
            dict(type='Person', name='John Doe'),
            dict(type='Person', name='Mary Jane'),
            dict(type='City', name='Gotham City')
        ]
        self.db.save(docs)

        # update the first doc to provoke a conflict in the next bulk update
        doc = docs[0].copy()
        self.db[doc['_id']] = doc

        conflicts = self.db.save(docs)
        self.assertTrue(doc['_id'] in conflicts.pending)
        # assert isinstance(results[0][2], io.Conflict)

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

        conflicts = self.db.save(docs, all_or_nothing=True)
        self.assertTrue(doc['_id'] not in conflicts)

        doc = self.db.get(doc['_id'], conflicts=True)
        assert '_conflicts' in doc
        revs = self.db.get(doc['_id'], open_revs='all')
        # print self.doc.name, doc['_id']
        assert len(revs) == 2

    def test_bulk_update_bad_doc(self):
        self.assertRaises(TypeError, self.db.save, [object()])

    def test_copy_doc(self):
        self.db['foo'] = {'status': 'testing'}
        result = self.db.copy('foo', 'bar')
        self.assertEqual(result, self.db['bar']._rev)

    def test_copy_doc_conflict(self):
        self.db['bar'] = {'status': 'idle'}
        self.db['foo'] = {'status': 'testing'}
        self.assertRaises(HTTPError, self.db.copy, 'foo', 'bar')

    def test_copy_doc_overwrite(self):
        self.db['bar'] = {'status': 'idle'}
        self.db['foo'] = {'status': 'testing'}
        result = self.db.copy('foo', self.db['bar'])
        doc = self.db['bar']
        self.assertEqual(result, doc._rev)
        self.assertEqual('testing', doc['status'])

    def test_copy_doc_srcobj(self):
        self.db['foo'] = {'status': 'testing'}
        self.db.copy(self.db['foo'], 'bar')
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_destobj_norev(self):
        self.db['foo'] = {'status': 'testing'}
        self.db.copy('foo', {'_id': 'bar'})
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_src_dictlike(self):
        class DictLike(object):
            def __init__(self, doc):
                self.doc = doc
            def items(self):
                return self.doc.items()
        self.db['foo'] = {'status': 'testing'}
        self.db.copy(DictLike(self.db['foo']), 'bar')
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_dest_dictlike(self):
        class DictLike(object):
            def __init__(self, doc):
                self.doc = doc
            def items(self):
                return self.doc.items()
        self.db['foo'] = {'status': 'testing'}
        self.db['bar'] = {}
        self.db.copy('foo', DictLike(self.db['bar']))
        self.assertEqual('testing', self.db['bar']['status'])

    def test_copy_doc_src_baddoc(self):
        self.assertRaises(TypeError, self.db.copy, object(), 'bar')

    def test_copy_doc_dest_baddoc(self):
        self.assertRaises(TypeError, self.db.copy, 'foo', object())

    def test_changes(self):
        self.db['foo'] = {'bar': True}
        changes = self.db.changes(since=0)
        self.assertEqual(changes['last_seq'], 1)
        first = changes['results'][0]
        self.assertEqual(first['seq'], 1)
        self.assertEqual(first['id'], 'foo')

    def test_changes_nofeed(self):
        try:
            self.db.changes(feed='continuous')
        except Exception, e:
            self.assertIsInstance(e, RuntimeError)
            return
        raise Exception("Exception should have been raised...")

    def test_purge(self):
        doc = {'a': 'b'}
        self.db['foo'] = doc
        self.assertEqual(self.db.purge([doc])['purge_seq'], 1)

    def test_json_encoding_error(self):
        doc = {'now': datetime.now()}
        self.assertRaises(TypeError, self.db.save, doc)


class ViewTestCase(testutil.TempDatabaseMixin, unittest.TestCase):

    def test_row_object(self):
        row = list(self.db.view('_all_docs', keys=['blah']))[0]
        self.assertEqual(repr(row), u"<Row \"blah\" / None «not_found»>".encode('utf-8'))
        self.assertEqual(row.id, None)
        self.assertEqual(row.key, 'blah')
        self.assertEqual(row.value, None)
        self.assertEqual(row.error, 'not_found')

        self.db.save({'_id': 'xyz', 'foo': 'bar'})
        row = list(self.db.view('_all_docs', keys=['xyz']))[0]
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

        res = list(self.db.view('test/multi_key', keys=range(1, 6, 2)))
        self.assertEqual(3, len(res))
        for idx, i in enumerate(range(1, 6, 2)):
            self.assertEqual(i, res[idx].key)

    def test_ddoc_info(self):
        self.db['_design/test'] = {
            'language': 'javascript',
            'views': {
                'test': {'map': 'function(doc) { emit(doc.type, null); }'}
            }
        }
        info = self.db.info('test')
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
        self.assertTrue(self.db.compact('test'))

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
        self.assertTrue(self.db.cleanup())

    def test_view_function_objects(self):
        if 'python' not in self.server.config()['query_servers']:
            return

        for i in range(1, 4):
            self.db.save({'i': i, 'j':2*i})

        def map_fun(doc):
            yield doc['i'], doc['j']
        res = list(self.db.query(map_fun, language='python'))
        self.assertEqual(3, len(res))
        for idx, i in enumerate(range(1,4)):
            self.assertEqual(i, res[idx].key)
            self.assertEqual(2*i, res[idx].value)

        def reduce_fun(keys, values):
            return sum(values)
        res = list(self.db.query(map_fun, reduce_fun, 'python'))
        self.assertEqual(1, len(res))
        self.assertEqual(12, res[0].value)

    def test_init_with_resource(self):
        self.db['foo'] = {}
        view = self.db.view('_all_docs')
        self.assertEquals(len(view), 1)

    def test_iter_view(self):
        self.db['foo'] = {}
        view = self.db.view('_all_docs')
        count = 0
        for r in view: count += 1
        self.assertEquals(count, 1)

    def test_tmpview_repr(self):
        mapfunc = "function(doc) {emit(null, null);}"
        view = self.db.query(mapfunc)
        self.assertTrue('_temp_view' in repr(view))
        # self.assertTrue(mapfunc in repr(view))

    def test_properties(self):
        for attr in ['rows', 'total_rows', 'offset']:
            self.assertTrue(getattr(self.db.view('_all_docs'), attr) is not None)

    def test_rowrepr(self):
        self.db['foo'] = {}
        rows = list(self.db.query("function(doc) {emit(\"something\", 1);}"))
        self.assertEquals('<Row "something" foo / 1>', repr(rows[0]))

        rows = list(self.db.query("function(doc) {emit(\"something\", \"nothing\");}", "function(keys, values, combine) {return values.slice(0,1);}"))
        reprstr = repr(rows[0])
        self.assertEquals("<Row / [u'nothing']>", repr(rows[0]))

class ShowListTestCase(testutil.TempDatabaseMixin, unittest.TestCase):

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
        super(ShowListTestCase, self).setUp()
        # Workaround for possible bug in CouchDB. Adding a timestamp avoids a
        # 409 Conflict error when pushing the same design doc that existed in a
        # now deleted database.
        design_doc = dict(self.design_doc)
        design_doc['timestamp'] = time.time()
        self.db.save(design_doc)
        self.db.save([{'_id': '1', 'name': 'one'}, {'_id': '2', 'name': 'two'}])

    def test_show_urls(self):
        self.assertEqual(self.db.show('_design/foo/_show/bar'), u'null:<default>')
        self.assertEqual(self.db.show('foo/bar'), u'null:<default>')

    def test_show_docid(self):
        self.assertEqual(self.db.show('foo/bar'), u'null:<default>')
        self.assertEqual(self.db.show('foo/bar', '1'), u'1:<default>')
        self.assertEqual(self.db.show('foo/bar', '2'), u'2:<default>')

    def test_show_params(self):
        self.assertEqual(self.db.show('foo/bar', r='abc'), u'null:abc')

    def test_list(self):
        self.assertEqual(self.db.list('foo/list', 'foo/by_id').body, u'1\r\n2\r\n')
        self.assertEqual(self.db.list('foo/list', 'foo/by_id', include_header='true').body, u'id\r\n1\r\n2\r\n')

    def test_list_keys(self):
        self.assertEqual(self.db.list('foo/list', 'foo/by_id', keys=['1']).body, u'1\r\n')

    def test_list_view_params(self):
        self.assertEqual(self.db.list('foo/list', 'foo/by_name', startkey='o', endkey='p').body, u'1\r\n')
        self.assertEqual(self.db.list('foo/list', 'foo/by_name', descending=True).body, u'2\r\n1\r\n')

class UpdateHandlerTestCase(testutil.TempDatabaseMixin, unittest.TestCase):
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
        super(UpdateHandlerTestCase, self).setUp()
        # Workaround for possible bug in CouchDB. Adding a timestamp avoids a
        # 409 Conflict error when pushing the same design doc that existed in a
        # now deleted database.
        design_doc = dict(self.design_doc)
        design_doc['timestamp'] = time.time()
        self.db.save(design_doc)
        self.db.save([{'_id': 'existed', 'name': 'bar'}])

    def test_empty_doc(self):
        # self.db.save()
        self.assertEqual(self.db.update('foo/bar'), 'empty doc')

    def test_new_doc(self):
        self.assertEqual(self.db.update('foo/bar', 'new'), 'new doc')

    def test_update_doc(self):
        self.assertEqual(self.db.update('foo/bar', 'existed'), 'hello doc')

# class URLsTestCase(testutil.TempDatabaseMixin, unittest.TestCase):
#    def test_unpack(self):
#        uri = 'http://user:pass@127.0.0.1/db'
#        print io.normalize_url(uri)
#        # url = io.urlunpack(uri)
#        # url.setdefault('scheme', 'https')
#        # from pprint import pprint
#        # pprint(url)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(CouchTests, 'test'))
    suite.addTest(unittest.makeSuite(CouchTestCase, 'test'))
    suite.addTest(unittest.makeSuite(DatabaseTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ViewTestCase, 'test'))
    suite.addTest(unittest.makeSuite(ShowListTestCase, 'test'))
    suite.addTest(unittest.makeSuite(UpdateHandlerTestCase, 'test'))
    # suite.addTest(unittest.makeSuite(URLsTestCase, 'test'))

    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
