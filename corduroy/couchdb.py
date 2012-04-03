# encoding: utf-8
"""
corduroy.couchdb

Basic python mapping of the CouchDB HTTP api.
"""

import os, re
import mimetypes
from urlparse import urlsplit, urlunsplit
from .io import Resource, ChangesFeed, quote, urlencode, is_relaxed
from .exceptions import HTTPError, PreconditionFailed, NotFound, ServerError, Unauthorized, \
                        Conflict, ConflictResolution
from .atoms import View, Row, Document, Status, adict, odict
from .config import defaults, json


__all__ = ['Couch', 'Database']

def NOOP(*args): return args
    
class Couch(object):
    """Represents a CouchDB server. 
    
    Useful for creating/deleting DBs and dealing with system-level functionality such
    as replication and task monitoring."""
    def __init__(self, url=None, auth=None, full_commit=True):
        """Initialize the server object.
        
        Args:
            url (str): url of the couchdb server

            auth (tuple): login information. e.g., ('username', 'password')

        Kwargs:
            full_commit (bool): include the X-Couch-Full-Commit header
        """        
        if url is None or isinstance(url, basestring):
            self.resource = Resource(url, auth=auth)
        else:
            self.resource = url # treat as a Resource object
        if not full_commit:
            self.resource.headers['X-Couch-Full-Commit'] = 'false'

    def __contains__(self, name):
        """Return whether the server contains a database with the specified
        name. (synchronous)
        """
        try:
            self.resource.head(validate_dbname(name))
            return True
        except NotFound:
            return False

    def __iter__(self):
        """Iterate over the names of all databases. (synchronous)"""
        data = self.resource.get_json('_all_dbs')
        return iter(data)

    def __len__(self):
        """Return the number of databases. (synchronous)"""
        data = self.resource.get_json('_all_dbs')
        return len(data)

    def __nonzero__(self):
        """Return whether the server is available. (synchronous)"""
        try:
            self.resource.head()
            return True
        except:
            return False

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.resource.url)

    def __delitem__(self, name):
        """Remove the database with the specified name. (synchronous)"""
        self.resource.delete_json(validate_dbname(name))

    def __getitem__(self, name):
        """Return a `Database` object representing the database with thespecified name.
        (synchronous)"""
        db = Database(self.resource(name))
        db.resource.head() # actually make a request to the database
        return db

    def config(self, name=None, value=None, delete=False, callback=None):
        """Get/set the configuration of the CouchDB server (or a field thereof).
        
        Args:
            name (str): optional path to a sub-field of the config dict

            value (str,dict,int): optional new value for the path specified by `name`

        Kwargs:
            delete (bool): if true, delete the path speficied by `name`
            
        Returns:
            When called without a `name` arg, returns the entire config dictionary. 

            When `name` is specified, returns the value of that sub-path

            When `value` is specified, returns None

            When `delete` is specified, returns the pre-deletion value
        """
        if delete:
            assert(value is None)
        
        if not name:
            resource = self.resource('_config')
        else:
            resource = self.resource('_config', *name.split('/'))
        
        if not value and delete is True:
            return resource.delete_json(callback=callback)
        if value:
            return resource.put_json(body=json.encode(value), callback=callback)
        else:
            return resource.get_json(callback=callback)

    def version(self, callback=None):
        """The version string of the CouchDB server.
        """
        def postproc(data, status):
            if status.ok:
                data = data['version']
            return data, status
        
        return self.resource.get_json(callback=callback, process=postproc)

    def stats(self, name=None, callback=None):
        """Server statistics.
        
        Args:
            name (str): optional sub-path of stats dict to return
            
        Returns:
            When called without args, returns the entire stats dictionary

            When `name` is specified, returns the value at that sub-path
            
        Raises:
            NotFound (when provided `name` is invalid)
        """
        if not name:
            resource = self.resource('_stats')
        else:
            resource = self.resource('_stats', *name.split('/'))
        return resource.get_json(callback=callback)

    def tasks(self, callback=None):
        """A list of tasks currently active on the server."""
        return self.resource.get_json('_active_tasks', callback=callback)

    def uuids(self, count=1, callback=None):
        """Retrieve a batch of uuids
        
        Args:
            count (int): optional number of uuids to retrieve

        Returns:
            list. A list of uuid strings of length=count
        """
        def postproc(data, status):
            if status.ok:
                data = data['uuids']
            return data, status
            
        return self.resource.get_json('_uuids', process=postproc, callback=callback, count=count)

    def db(self, name, create_if_missing=False, callback=None):
        """Initialize a Database object corrsponding to a particular db name.
        
        Args:
            name (str): The name of the database (without the server's url prefix)

            create_if_missing (bool): If true, will handle NotFound errors by creating
            the database specified by `name`

        Kwargs:
            create_if_missing (bool): if True, attempt to create the database if the
            initial request results in a NotFound
            
        Returns:
            Database. An initialized Database object
            
        Raises:
            NotFound (when database does not exists and create_if_missing==False)
        """
        _db = Database(self.resource(name))
        def handle_missing(data, status):
            if status.ok:
                data = _db
            elif status.error is NotFound and create_if_missing:
                return self.create(name, callback=callback)
            elif callback:
                callback(data, status)
            return data, status

        if callback:
            return _db.resource.get_json(callback=handle_missing)
        else:
            try:
                return _db.resource.get_json(process=handle_missing)
            except NotFound:
                if not create_if_missing: raise
                return self.create(name)

    def all_dbs(self, callback=None):
        """Retrieve the list of database names on this server"""
        return self.resource.get_json('_all_dbs', callback=callback)

    def create(self, name, callback=None):
        """Create a new database with the given name.
        
        Args:
            name (str): The name of the database to create (without the server's url prefix)
            
        Returns:
            Database. An initialized Database object

        Raises:
            PreconditionFailed (if the database already exists)
        """
        def postproc(data, status):
            if status.ok: 
                db = Database(self.resource(name))
                data = db
            return data, status
        return self.resource.put_json(validate_dbname(name), process=postproc, callback=callback)

    def delete(self, name, callback=None):
        """Delete the database with the specified name.

        Args:
            name (str): The name of the database to delete

        Raises:
            NotFound (when database does not exists)
        """
        return self.resource.delete_json(validate_dbname(name), callback=callback)

    def replicate(self, source, target, callback=None, **options):
        """Replicate changes from the source database to the target database.
        
        Args:
            source (str, Database): either a full url to the source database (with authentication
            provided inline) or an initialized Database object

            target (str, Database): the url or Database object of the target db
            
        Kwargs:
            _id (str): an optional replication_id. If provided, a doc will be created in
            the `_replicator` database for subsequent querying. If not provided, the  
            legacy `_replicate` API will be used instead.

            cancel (bool): if true, cancel the replication

            continuous (bool): if True, set the replication to be continuous

            create_target (bool): if True, creates the target database

            doc_ids (list): optional list of document IDs to be synchronized

            proxy (str): optional address of a proxy server to use
        """
        if hasattr(source,'resource'):
            source = source.resource.auth_url
        if hasattr(target,'resource'):
            target = target.resource.auth_url
            
        data = {'source': source, 'target': target}
        data.update(options)
        if '_id' in options:
            return self.resource.post_json('_replicator', data, callback=callback)
        else:
            return self.resource.post_json('_replicate', data, callback=callback)

    @property
    def users(self):
        """The _users system database.
        
        Returns:
            Database. This property is a synonym for `self.db('_users')`
        """
        return Database(self.resource('_users'))

    @property
    def replicator(self):
        """The _replicator system database
        
        Returns:
            Database. This property is a synonym for `self.db('_replicator')`
        """
        return Database(self.resource('_replicator'))

class Database(object):
    """Represents a single DB on a couch server. 
    
    This is the primary class for interacting with documents, views, changes, et al."""
    def __init__(self, name, auth=None):
        """Initialize the database object.
        
        Args:
            name (str): either a full url path to the database, or just a database name 
            (to which the host specified in corduroy.defaults will be prepended)
            
            auth (tuple): optional login information. e.g., ('username', 'password')
        """        
        if isinstance(name, basestring):
            self.resource = Resource(name, auth=auth)
        elif isinstance(name, Resource):
            self.resource = name
        else:
            raise ValueError('expected str, got %s'%type(name))
            
        self.name = validate_dbname(self.resource.url.split('/')[-1], encoded=True)
        self._uuids = []

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.name)

    def __contains__(self, id):
        """Return whether the database contains a document with the specified
        ID. (synchronous)
        """
        try:
            data, status = _doc_resource(self.resource, id).head()
            return True
        except NotFound:
            return False

    def __iter__(self):
        """Return the IDs of all documents in the database. (synchronous)"""
        return iter([item.id for item in self.view('_all_docs')])

    def __len__(self):
        """Return the number of documents in the database. (synchronous)"""
        data = self.resource.get_json()
        return data['doc_count']

    def __nonzero__(self):
        """Return whether the database is available. (synchronous)"""
        try:
            self.resource.head()
            return True
        except:
            return False

    def __delitem__(self, id):
        """Remove the document with the specified ID from the database. (synchronous)
        """
        resource = _doc_resource(self.resource, id)
        resp = resource.head()
        
        def postproc(data, status):
            result = resource.delete_json(rev=status.headers['etag'].strip('"'))
            return result, status
        return resource.head(process=postproc)

    def __getitem__(self, id):
        """Return the document with the specified ID. (synchronous)
        """
        data = _doc_resource(self.resource, id).get_json()
        return defaults.types.doc(data)

    def __setitem__(self, id, content):
        """Create or update a document with the specified ID. (synchronous)
        """
        # content.setdefault('_id', id)
        content['_id'] = id
        return self.save(content)

    def exists(self, callback=None):
        """Check whether this Database object corresponds to an existing db on the server
        
        Returns:
           boolean
        """
        def postproc(data, status):
            data = status.error is not NotFound
            return data, status
            
        try:
            return self.resource.head(process=postproc, callback=callback)
        except NotFound:
            return False
        
        # def postproc(data, status):
        #     data = status.error is not NotFound
        #     return data, status
        # data = self.resource.head(process=postproc, callback=callback)

    @property
    def _couch(self):
        """Creates an instance of the parent Couch object (ici il y avoir des dragons)"""
        parts = urlsplit(self.resource.auth_url)
        path = "/".join(parts.path.split('/')[:-1])
        parts = list(parts)
        parts[2] = path
        return Couch(urlunsplit(tuple(parts)))

    def push(self, target, callback=None, **options):
        """Initiate a replication from this database to a target url
        
        Args:
            target (str, Database): the target database

        Kwargs (c.f., Couch.replicate):
            _id (str): an optional replication_id. If provided, a doc will be created in
            the `_replicator` database for subsequent querying. If not provided, the  
            legacy `_replicate` API will be used instead.

            cancel (bool): if true, cancel the replication

            continuous (bool): if True, set the replication to be continuous

            create_target (bool): if True, creates the target database

            doc_ids (list): optional list of document IDs to be synchronized

            proxy (str): optional address of a proxy server to use
        """
        return self._couch.replicate(self, target, callback=callback, **options)
        
    def pull(self, source, callback=None, **options):
        """Initiate a replication from a source url to this database
        
        Args:
            source (str, Database): the database from which to replicate

        Kwargs (c.f., Couch.replicate):
            _id (str): an optional replication_id. If provided, a doc will be created in
            the `_replicator` database for subsequent querying. If not provided, the  
            legacy `_replicate` API will be used instead.

            cancel (bool): if true, cancel the replication

            continuous (bool): if True, set the replication to be continuous

            create_target (bool): if True, creates the target database

            doc_ids (list): optional list of document IDs to be synchronized

            proxy (str): optional address of a proxy server to use
        """
        return self._couch.replicate(source, self, callback=callback, **options)
        
    def cleanup(self, callback=None):
        """Clean up old design document indexes.
        
        Returns:
            dict of the form `{ok:True}`
        """
        def postproc(data, status):
            if status.ok:
                data=data['ok']
            return data, status

        headers = {'Content-Type': 'application/json'}
        return self.resource('_view_cleanup').post_json(headers=headers, process=postproc, callback=callback)

    def commit(self, callback=None):
        """If the server is configured to delay commits, or previous requests
        used the special ``X-Couch-Full-Commit: false`` header to disable
        immediate commits, this method can be used to ensure that any
        non-committed changes are committed to physical storage.
        """
        headers={'Content-Type': 'application/json'}
        return self.resource.post_json('_ensure_full_commit', headers=headers, callback=callback)

    def compact(self, ddoc=None, callback=None):
        """Compact the database or a design document's index.
        
        Args:
            ddoc (str): optional design doc name
        """
        if ddoc:
            resource = self.resource('_compact', ddoc)
        else:
            resource = self.resource('_compact')
        headers={'Content-Type': 'application/json'}
        def postproc(data, status):
            if status.ok: # do i need the error checking here? for async maybe? o_O
                data=data['ok']
            return data, status
        return resource.post_json(headers=headers, process=postproc, callback=callback)

    def security(self, value=None, callback=None):
        """Access the database's _security object
        
        If called without arguments, returns the security object. If called with a 
        dict, sets the security object to that value.
        
        Args:
            value (dict): the new security object to be saved to the database
        
        Returns:
            dict. The current security object.
        """
        if hasattr(value,'items'):
            def postproc(data, status):
                if status.ok:
                    data=value
                return data, status
            headers = {'Content-Type': 'application/json'}
            return self.resource.put_json('_security', body=value, headers=headers, process=postproc, callback=callback)
        else:
            return self.resource.get_json('_security', callback=callback)

    def _bulk_get(self, doc_ids, include_docs=True, process=None, callback=None, **options):
        """Perform a bulk fetch of documents (ici il y avoir des dragons)"""
        
        propterhoc = process or NOOP
        def posthoc(view, status):
            # pull the docs out of their rows and build a list of docs and Nones
            # depending on whether the fetch was successful
            data = view
            if status.ok:
                if include_docs:
                    docs = [row.doc for row in view]
                    missing = sum(1 for d in docs if d is None)
                    data = docs
                else:
                    vals = [(row.key, row.value) for row in view]
                    missing = sum(1 for v in vals if 'error' in v[1])

                    doc_stubs = []
                    for doc_id, doc_val in vals:
                        doc_stubs.append(defaults.types.doc(
                          _id=doc_id, _rev=doc_val.get('rev'), _stub=True
                        ))
                        
                    data = doc_stubs
            return propterhoc(data, status)


        if doc_ids is False:
            # using false here from the default value in .get (avoiding None in case a user variable thought
            # to be containing a string gets passed as the doc id. best not to do a full-db get in response...)
            options.setdefault('limit',50)
            return self.view('_all_docs', include_docs=include_docs, process=posthoc, callback=callback, **options)
        return self.view('_all_docs',keys=doc_ids, include_docs=include_docs, process=posthoc, callback=callback, **options)

    def get(self, id_or_ids=False, callback=None, **options):
        """Return the document(s) with the specified ID(s).
        
        Args:
            id_or_ids (str, list): either a single ID or a list of IDs to fetch in a bulk request
            
        Kwargs:
            rev (str): if present, specifies a specific revision to retrieve
        
            revs (bool): if true, add a _revs attribute to the returned doc
        
            revs_info (bool): if true, add a _revs_info attribute to the returned doc

            When called with a list of IDs, all standard view options can be applied
            (see Database.view for a complete listing).

        Returns:
            If a single ID is requested, returns a corresponding Document or raises an 
            exception if the doc does not exist.

            If a list of IDs was requested, returns a corresponding list of Document objects.
            Requesting IDs referencing non-existent docs will not raise an exception but will
            place a corresponding None in the list of returned docs.
            
        Raises:
            NotFound (when a single ID is requested and no corresponding doc is found)
        """
        if not isinstance(id_or_ids, basestring):
            return self._bulk_get(id_or_ids, callback=callback, **options)
        
        def postproc(data, status):
            if status.ok:
                if isinstance(data, (list,tuple)):
                    data = [defaults.types.doc(d) for d in data]
                else:
                    data = defaults.types.doc(data)
            return data, status
        return _doc_resource(self.resource, id_or_ids).get_json(process=postproc, callback=callback, **options)


    def _solo_save(self, doc, force=False, merge=None, callback=None, **options):
        """Perform a single-document update (ici il y avoir des dragons)"""
        if '_id' in doc:
            put_or_post = _doc_resource(self.resource, doc['_id']).put_json
        else:
            put_or_post = self.resource.post_json
            
        def soloproc(data, status):
            if status.ok:
                id, rev = data['id'], data.get('rev')
                doc['_id'] = id
                if rev is not None: # Not present for batch='ok'
                    doc['_rev'] = rev
                data = ConflictResolution(self, [data], [doc])
            else:
                conflicts = ConflictResolution(self, [status.response], [doc])
                if force:
                    return conflicts.overwrite(callback=callback), status
                elif merge:
                    return conflicts.merge(merge, callback=callback), status
                data = conflicts    
                
            if status.error is Conflict:
                status.exception = data
            if callback:
                callback(data, status)
            return data, status

        if callback:
            return put_or_post(body=doc, callback=soloproc, **options)
        else:
            return put_or_post(body=doc, process=soloproc, **options)

    def _bulk_save(self, docs, force=False, merge=None, callback=None, **options):
        """Perform a multi-document update (ici il y avoir des dragons)"""

        to_post = []
        for doc in docs:
            if isinstance(doc, dict):
                to_post.append(doc)
            elif hasattr(doc, 'items'):
                to_post.append(dict(doc.items()))
            else:
                raise TypeError('expected dict, got %s' % type(doc))

        def bulkproc(data, status):
            # print "[%i]"%status.code
            handle_remaining = callback or NOOP
            if status.ok:
                conflicts = ConflictResolution(self, data, docs)
                data = conflicts
                if conflicts.pending:
                    if force:
                        return conflicts.overwrite(callback=callback), status
                    elif merge:
                        return conflicts.merge(merge,callback=callback), status
            
            # no conflicts, returning after the single round-trip
            return handle_remaining(data, status)
        
        content = dict(docs=to_post)
        content.update(options)

        cb = proc = None
        if callback: cb = bulkproc
        else: proc = bulkproc
        return self.resource.post_json('_bulk_docs', body=content, process=proc, callback=cb, **options)
    
    def save(self, doc_or_docs=None, merge=None, force=False, callback=None, **options):
        """Create a new document or update an existing document.
        
        Args:
            doc_or_docs (dict, list): either a single dict-like object or a list of many
        
        Kwargs:
            batch (str): if equal to "ok" when submitting a single document, the server will
            defer writing to disk (speeding throughput but risking data loss)
            
            all_or_nothing (boolean): if True when submitting a list of documents, conflict
            checking will be disabled and either the entire list of docs will be written
            to the database or none will (see couchdb docs for more details on the not-
            entirely-intuitive semantics).
            
            force (bool): if True, will retry any writes that caused a conflict after fetching
            the current _revs from the server. This will not necessarily succeed if the database
            is write-heavy due to the race condition between retrieving the _rev and attempting 
            the update. The use of force outside of a debugging context is highly discouraged.
            
            merge (function w/ signature ƒ(local_doc, server_doc)): If the inital update request
            causes any conflicts, the current server copy of each conflicting doc will be
            fetched and the `merge` function will be called for each local/remote pair. The
            merge function should return either a dict-like object to be written to the database
            or (in case the write attempt should be abandoned) None.
            
        Side Effects:
            All docs passed as arguments will have their _id and/or _rev updated to reflect a 
            successful write. In addition, these up-to-date dicts can be found in the 
            return value's `.resolved` property (useful in an async context where the callback
            function doesn't have the original arguments in its scope)
        
        Returns:
            ConflictResolution. An object with two attributes of interest:
                * pending: a dictionary (keyed by _id) of docs which were not successfully
                           written due to conflicts
                * resolved: a dictionary (keyed by _id) of docs which were successfully written
                
        Raises:
            Conflict.
            
            As with the Database.get method, exceptions will be raised in the single-doc case, 
            but when dealing with a list of docs, errors will be signaled on a doc-by-doc basis
            in the return value.
        """
        # if we're being called from a generator, create a Task and then re-call ourselves
        # using its callback
        if not callback and is_relaxed():
            from tornado import gen
            def multipass(callback):
                gen_callback = callback # supplied by gen.Task
                def unpack_results(data, status):
                    gen_callback(data) if status.ok else gen_callback(status.exception)
                self.save(doc_or_docs, merge=merge, force=force, callback=unpack_results, **options)
            return gen.Task(multipass)

        # look for missing _id fields
        orphans = []
        _save = None
        _couch = self._couch
        if isinstance(doc_or_docs, (list, tuple)):
            orphans = [doc for doc in doc_or_docs if '_id' not in doc]
            _save = self._bulk_save
        elif hasattr(doc_or_docs,'items'):
            orphans = [doc_or_docs] if '_id' not in doc_or_docs else []
            _save = self._solo_save
        else:
            raise TypeError('expected dict or list, got %s' % type(doc_or_docs))

        # raise an exception if the docs arg isn't serializeable, would be nice to
        # know if this is as wasteful as it feels...
        json.encode(doc_or_docs)

        # fill in missing _ids with cached/fetched uuids then proceed with the save
        if len(orphans) > len(self._uuids):
            def decorate_uuids(data, status):
                if status.ok:
                    self._uuids.extend(data['uuids'])
                    for doc, uuid in zip(orphans, self._uuids):
                        doc['_id'] = uuid
                    self._uuids = self._uuids[len(orphans):]
                    return _save(doc_or_docs, force=force, merge=merge, callback=callback, **options), status
                else:
                    return data, status

            cb = proc = None
            if callback: cb = decorate_uuids
            else: proc = decorate_uuids
            return _couch.resource.get_json('_uuids', callback=cb, process=proc, count=max(len(orphans), defaults.uuid_cache))
            
        else:
            if orphans:
                for doc, uuid in zip(orphans, self._uuids):
                    doc['_id'] = uuid
                self._uuids = self._uuids[len(orphans):]
            return _save(doc_or_docs, force=force, merge=merge, callback=callback, **options)

    def copy(self, source, dest, callback=None):
        """Copy a given document to create a new document or overwrite an old one.
        
        Args:
            source (str, dict): either a string containing an ID or a dict with `_id` and `_rev` keys
            specifying the document to be copied
            
            dest (str, dict): either a string containing an ID or a dict with `_id` and `_rev` keys
            specifying the document to be created/overwritten
            
        Returns:
            dict of form {id:'', rev:''} identifying the destination document.
            
        Raises:
            Conflict (when dest doc already exists and an up-to-date _rev was not specified)
        """
        src = source
        if not isinstance(src, basestring):
            if not isinstance(src, dict):
                if hasattr(src, 'items'):
                    src = dict(src.items())
                else:
                    raise TypeError('expected dict or string, got %s' %
                                    type(src))
            src = src['_id']

        if not isinstance(dest, basestring):
            if not isinstance(dest, dict):
                if hasattr(dest, 'items'):
                    dest = dict(dest.items())
                else:
                    raise TypeError('expected dict or string, got %s' %
                                    type(dest))
            if '_rev' in dest:
                dest = '%s?%s' % (quote(dest['_id']),
                                  urlencode({'rev': dest['_rev']}))
            else:
                dest = quote(dest['_id'])
                
        def postproc(data, status):
            if status.ok: 
                data = data['rev']
            return data, status

        return self.resource._request_json('COPY', src, headers={'Destination': dest},
                                                process=postproc, callback=callback)
            
    def delete(self, doc, callback=None):
        """Delete the given document from the database.
        
        Args:
            doc (dict-like): an object with `_id` and `_rev` keys identifying the doc to be deleted
        
        Returns:
            dict: {ok:True, id:'', rev:''}
            
        Raises:
            NotFound (when doc['_id'] does not exist)
            Conflict (when dest doc already exists and an up-to-date _rev was not specified)
        """
        if doc['_id'] is None:
            raise ValueError('document ID cannot be None')
        headers={'Content-Type': 'application/json'}
        
        # TODO *could* have it return the doc but with _deleted=True appended...
        return _doc_resource(self.resource, doc['_id']).delete_json(rev=doc['_rev'], headers=headers, callback=callback)


    def revisions(self, id, callback=None, **options):
        """Return all available revisions of the given document.
        
        Args:
            id (str): ID of the doc whose revisions to fetch
            
        Returns:
            list. All prior _rev strings sorted reverse chronologically
            
        Raises:
            NotFound.
        """
        def postproc(data, status):
            if status.ok:
                history = []
                startrev = data['_revisions']['start']
                for index, rev in enumerate(data['_revisions']['ids']):
                    history.append('%d-%s' % (startrev - index, rev))
                data = history
            return data, status
        resource = _doc_resource(self.resource, id)
        return resource.get_json(revs=True, process=postproc, callback=callback)

    def info(self, ddoc=None, callback=None):
        """Fetch information about the database or a design document.
        
        Args:
            ddoc (str): optional design doc name

        Returns:
            dict. Equivalent to a GET on the database or ddoc's url
        
        Raises:
            NotFound
        """
        if ddoc is not None:
            return self.resource('_design', ddoc, '_info').get_json(callback=callback)
        else:
            def postproc(data, status):
                self.name = data['db_name']
                return data, status
            return self.resource.get_json(process=postproc, callback=callback)

    def delete_attachment(self, doc, filename, callback=None):
        """Delete the specified attachment.
        
        Args:
            doc (dict-like): an object with `_id` and `_rev` keys
            
            filename (str): the name of the attachment to be deleted in the given doc
            
        Side Effects:
            Will update the doc argument's _rev value upon succesfully deleting the attachment

        Returns:
            dict {ok:True}

        Raises:
            NotFound, Conflict.
        """
        def postproc(data, status):
            if status.ok:
                rev = data.get('rev')
                if rev is not None:
                    doc['_rev'] = rev
                del doc['_attachments'][filename]
                if not doc['_attachments'].keys():
                    del doc['_attachments']
                data = doc
            return data, status
        resource = _doc_resource(self.resource, doc['_id'])
        return resource.delete_json(filename, rev=doc['_rev'], process=postproc, callback=callback)

    def get_attachment(self, id_or_doc, filename, callback=None):
        """Return an attachment from the specified doc and filename.
        
        Args:
            doc (str, dict-like): an ID string or dict with an `_id` key
            
            filename (str): the name of the attachment to retrieve
            
        Returns:
            str. The raw attachment data as a bytestring
        """
        if isinstance(id_or_doc, basestring):
            _id = id_or_doc
        else:
            _id = id_or_doc['_id']
            
        return _doc_resource(self.resource, _id).get(filename, callback=callback)

    def put_attachment(self, doc, content, filename=None, content_type=None, callback=None):
        """Create or replace an attachment.
        
        Args:
            doc (dict-like): an object with `_id` and `_rev` keys
        
            content (str, file): the attachment data
            
            filename (str): optionally specify the name to use (unnecessary w/ file objects)

            content_type (str): optionally specify the mime type to use (unnecessary w/ file objects)
        
        Side Effects:
            Will update the doc argument's _rev value upon succesfully updating the attachment
        
        Returns:
            dict of form `{ok:True, id:'', rev:''}`
            
        Raises:
            NotFound, Conflict
        """
        
        if filename is None:
            if hasattr(content, 'name'):
                filename = os.path.basename(content.name)
            else:
                raise ValueError('no filename specified for attachment')
        if content_type is None:
            content_type = ';'.join(
                filter(None, mimetypes.guess_type(filename))
            )
            
        def postproc(data, status):
            if status.ok:
                doc['_rev'] = data['rev']
                _attch = doc.get('_attachments',adict())
                _attch[filename] = dict(content_type=content_type, stub=True, added=True)
                doc['_attachments'] = _attch
                data = doc
            return data, status
        resource = _doc_resource(self.resource, doc['_id'])
        headers={'Content-Type': content_type}
        return resource.put_json(filename, body=content, headers=headers, rev=doc['_rev'],
                                                  process=postproc, callback=callback)


    def purge(self, docs, callback=None):
        """Perform purging (complete removal) of the given documents.

        Uses a single HTTP request to purge all given documents. Purged
        documents do not leave any metadata in the storage and are not
        replicated.
        
        Think thrice before doing this.
        
        Args:
            docs (list): containing dicts of the form `{_id:'', _rev:''}`
        
        Returns:
            dict of the form `{purge_seq:0, purged:{id1:[], id2:[], ...}}`
        """
        content = {}
        for doc in docs if not hasattr(docs, 'items') else [docs]:
            if isinstance(doc, dict):
                content[doc['_id']] = [doc['_rev']]
            elif hasattr(doc, 'items'):
                doc = dict(doc.items())
                content[doc['_id']] = [doc['_rev']]
            else:
                raise TypeError('expected dict, got %s' % type(doc))
        return self.resource.post_json('_purge', body=content, callback=callback)


    def show(self, name, id=None, callback=None, **options):
        """Call a 'show' function.
        
        Args:
            name (str): the show function to use (e.g., myddoc/atomfeed)
            
            id (str): optional ID of the doc on which the show function will 
                      be run

        Returns:
            object with two attributes of interest:
                * headers: a dictionary of the response headers
                * body: either a bytestring or a decoded json object (if the 
                        response content type was application/json)
        """
        path = _path_from_name(name, '_show')
        if id:
            path.append(id)
        def postproc(data, status):
            if status.ok:
                if status.headers.get('content-type') == 'application/json':
                    body = json.decode(data)
                data = adict(body=body, headers=status.headers)
            return data, status
        
        return self.resource(*path).get(callback=callback, **options)
        

    def list(self, name, view, callback=None, **options):
        """Format a view using a 'list' function.
        
        Args:
            name (str): the ddoc and list function name (e.g., myddoc/weekly)
            view (str): a view to run the list against. if the view is in
                        the same ddoc as the list function, just its name can
                        be passed (e.g., 'stats' instead of 'myddoc/stats'). 
                        Otherwise the ddoc should be included in the view name.

        Returns:
            object with two attributes of interest:
                * headers: a dictionary of the response headers
                * body: either a bytestring or a decoded json object (if the response
                        content type was application/json)
        
        """
        path = _path_from_name(name, '_list')
        path.extend(view.split('/', 1))
        opts = _encode_view_options(options)

        # return a {body:, headers:} dict where body is either a string or (if
        # the content-type was json) a decoded dict
        def postproc(data, status):
            body = data
            if status.ok:
                if status.headers.get('content-type') == 'application/json':
                    body = json.decode(data)
            data = adict(body=body, headers=status.headers)
            return data, status
        return self.resource(*path).get(process=postproc, callback=callback, **opts)

    def update(self, name, id=None, body=None, callback=None, **options):
        """Calls a server side update handler.
        
        Args:
            name (str): the update-handler function name (e.g., myddoc/in_place_update)
            
            id (str): optionally specify the ID of a doc to update
            
        Kwargs:
            body (str, dict): optionally include data in the POST body. Dicts will
            be form-encoded and will appear in your update handler in the req.form 
            field. Strings will be passed as-is and can be found in req.body.
            
            Other kwargs will be urlencoded and appended to the query string.
        """
        path = _path_from_name(name, '_update')
        if id is None:
            func = self.resource(*path).post
        else:
            path.append(id)
            func = self.resource(*path).put
        
        headers = {}
        if hasattr(body, 'items'):
            body = urlencode(body)
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
        return func(callback=callback, body=body, headers=headers, **options)
        
    def changes(self, callback=None, **opts):
        """Retrieve a list of changes from the database or begin listening to
        a continuous feed.

        Kwargs:
            since (int): 
                the earliest seq value that should be reported

            limit (int): 
                maximum number of changes to return

            filter (str): 
                name of a filter function (e.g., `myddoc/subset`)

            include_docs (bool): 
                if true, each element of the 'results' array in the return 
                value will also containt a 'doc' attribute
                        
            feed (str): 
                if 'continuous', a `ChangesFeed` object will be created and begin 
                listening to the specified _changes feed. 
            
            callback (function w/ signature ƒ(seq, changes)): 
                the callback will be invoked repeatedly whenever new changes 
                arrive. The seq argument is the integer value of the most recent 
                seq in that batch. the changes argument is a list of dicts of 
                the form: `[{seq:0, id:'', changes:[]}, …]`
            
            latency (default=1):             
                minimum time period between invocations of the callback. When
                set to 0, the callback will be invoked for every individual 
                change. With higher values, changes will be batched for
                efficiency's sake.
            
            heartbeat (int): 
                time period between keepalive events (in seconds).
            
            timeout (int): 
                maximum period of inactivity (in seconds) before which the server 
                will send a response.
            
        Returns:
            When called without requesting a feed: a dictionary of the form `{last_seq:0, results:[{id:'', seq:0, …}, …]}`
            
            If called with `feed='continuous'` and a valid callback: a ChangesFeed object 
        """
        if opts.get('feed') == 'continuous':
            if not hasattr(callback, '__call__'):
                raise RuntimeError('Continuous changes feed requires a callback argument')
            return ChangesFeed(self, callback=callback, **opts)
        return self.resource.get_json('_changes', callback=callback, **opts)


    def query(self, map_src, reduce_src=None, callback=None, **options):
        """Create a temporary view using the provided javascript function(s)
        and perform a mind-bendingly inefficient ad-hoc query.
        
        Args:
            map_src (str): a map function string such as:
                'function(doc){emit(null,null)}'
            
            reduce_src (str): optionally include a reduce function string:
                'function(key, values, rereduce){return sum(values)}'
                or the name of a builtin (`_sum`, `_count`, or `_stats`)
                
        Kwargs:
            all standard view options (see Database.view)
            
        Returns:
            View. An iterable list of Row object.
        """
        
        body = dict(map=map_src, language='javascript')

        if reduce_src:
            body['reduce'] = reduce_src
        viewkeys = options.pop('keys', None)
        opts = _encode_view_options(options)
        if viewkeys:
            body['keys'] = viewkeys
        content = json.encode(body)
        headers = {'Content-Type': 'application/json'}

        def postproc(data, status):
            # print "raw ««%s»»"%data
            if status.ok:
                data = View('_temp_view', options, data)
            return data, status
            
        return self.resource('_temp_view').post_json(body=content, headers=headers, process=postproc, callback=callback, **opts)
        

    def view(self, name, callback=None, **options):
        """Query a view.

        All of the query args in the HTTP api can be passed as keyword
        args. Key values should be json-serializeable objects (abbreviated
        as `obj` below) of the form defined in your view function.
        
        Args:
            name (str): a view name of the form 'myddoc/viewname'
            
        Kwargs:
            key (obj): retrieve only rows matching this key
            
            keys (list): a list of key values to retrieve

            descending (bool): whether to invert the ordering of the rows.
            This ordering is applied before any key filtering takes place,
            thus you may need to reverse your `start`s and `end`s when
            toggling this.

            startkey (obj): key of the first row to include in the results

            endkey (obj): key of the final row of results
            
            inclusive_end (bool): by default, include rows matching `endkey`
            in the results. If False, treat `endkey` as defining the
            first rows to be *excluded* from the results.

            startkey_docid (str): within the rows bounded by startkey and 
            endkey, perform a further filtering by discarding rows before
            this ID.
            
            endkey_docid (str): discard rows between this doc ID and endkey            
            
            include_docs (bool): if True, each Row in the results will
            have the corresponding document in its .doc attr.
            
            limit (int): the maximum number of rows to retrieve
            
            stale (str): specify how to handle view indexing,
                * 'ok': build results from the current index even if it's
                      out of date
                * 'update_after': return stale results but trigger a
                      view re-index for the benefit of subsequent queries.
            
            skip (int): of the rows that would be returned on the basis of
            any prior key filtering, discard this many from the beginning.
            
            update_seq (bool): include an update_seq field in the response
            indicating the seq of the most recent indexing (from which the
            results were pulled).

            reduce (bool): if False, ignore the reduce function on this 
            view and return the output of its map step.
            
            group (bool): if True, generate a row for each distinct key in
            the reduce results. By defualt, the reduce function's output
            will boil down to a single row.
            
            group_level (int or 'exact'): when using ‘complex keys’ (i.e., 
            lists) group_level defines how many elements from each key
            should be used when deciding if rows have ‘distinct’ keys
            for the purposes of the reduction step.
        
        Returns:
            View.
            
        """
        path = _path_from_name(name, '_view')
        
        propterhoc = options.get('process',NOOP)
        if propterhoc is not NOOP:
            del options['process']
        def posthoc(data, status):
            if status.ok:
                data = View(name, options, data)
            return propterhoc(data, status)

        viewkeys = options.pop('keys', None)
        opts = _encode_view_options(options)
        if viewkeys:
            return self.resource(*path).post_json(body=dict(keys=viewkeys), process=posthoc, callback=callback, **opts)
        else:
            return self.resource(*path).get_json(process=posthoc, callback=callback, **opts)




def _doc_resource(base, doc_id):
    """Return the resource for the given document id.
    """
    # Split an id that starts with a reserved segment, e.g. _design/foo, so
    # that the / that follows the 1st segment does not get escaped.

    try:
        if doc_id[:1] == '_':
            return base(*doc_id.split('/', 1))
        return base(doc_id)
    except Exception, e:
        tron()

def _path_from_name(name, type):
    """Expand a 'design/foo' style name to its full path as a list of
    segments.
    """
    if name.startswith('_'):
        return name.split('/')
    design, name = name.split('/', 1)
    return ['_design', design, type, name]


def _encode_view_options(options):
    """Encode any items in the options dict that are sent as a JSON string to a
    view/list function.
    """
    retval = {}
    for name, value in options.items():
        if name in ('key', 'startkey', 'endkey') or not isinstance(value, basestring):
            value = json.encode(value)
        retval[name] = value
    return retval


SPECIAL_DB_NAMES = set(['_users'])
VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+-/]*$')
def validate_dbname(name, encoded=False):
    if encoded:
        from urllib import unquote
        name = unquote(name)
    if name in SPECIAL_DB_NAMES:
        return name
    if not VALID_DB_NAME.match(name):
        raise ValueError('Invalid database name')
    return name
