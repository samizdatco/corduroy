# encoding: utf-8
"""
corduroy.exceptions

Everything that can go wrong...
"""

from __future__ import with_statement
import sys
import os
from .atoms import adict, odict, Document
from .config import defaults

class HTTPError(Exception):
    """Base class for errors based on HTTP status codes >= 400."""        

class PreconditionFailed(HTTPError):
    """Exception raised when a 412 HTTP error is received in response to a
    request.
    """

class NotFound(HTTPError):
    """Exception raised when a 404 HTTP error is received in response to a
    request.
    """

class ServerError(HTTPError):
    """Exception raised when an unexpected HTTP error is received in response
    to a request.
    """

class Unauthorized(HTTPError):
    """Exception raised when the server requires authentication credentials
    but either none are provided, or they are incorrect.
    """

class Conflict(HTTPError):
    """Exception raised when a 409 HTTP error is received in response to a
    request."""

class ConflictResolution(Conflict):
    """The result of an attempt to write to the database.
    
    Presents the results of a `Database.save` call and provides syntax 
    for resolution of any conflicted documents
    
    Attributes:
        pending (dict): A dictionary (keyed by _id) of docs that were not 
            successfully written due to conflicts. e.g., 
            ``{some_doc: {doc:{_id:"some_doc", …}, error:"conflict"}, …}``
            
        resolved (dict): A dictionary (keyed by `_id`) of docs that were written 
            without error. Their `_id` and `_rev` keys have been updated to 
            reflect the save.
    """    
    def __init__(self, db, bulk_docs_response, originals=None):
        self._db = db
        self._originals = originals or []
        self.pending = odict()
        self.resolved = odict()
        
        for result, orig in zip(bulk_docs_response, originals):
            doc_id = orig.get('_id', result.get('id'))
            if doc_id:
                self.pending[doc_id] = adict(doc=orig)
    
        self._reflect_bulk_post(bulk_docs_response, originals)

    def __repr__(self):
        conflicted = ", ".join(sorted(self.pending.keys()))
        nu = len(self.resolved)
        _s = lambda lst: '' if lst==1 else 's'
        
        if conflicted:
            nc = len(self.pending)
            tot = nu+nc
            return "<Conflict%s: %s>"%(_s(nc), conflicted)
            # return "<%i of %i doc%s written, conflict%s: %s>"%(nu, tot, _s(tot), _s(nc), conflicted)
        else:
            return "<Success: %i doc%s updated>"%(nu, _s(nu))

    def _reflect_bulk_post(self, resp, posted_docs):
        for result, orig in zip(resp, posted_docs):
            if 'id' in result:
                orig['_id'] = result['id']
            if '_deleted' in result:
                orig['_deleted'] = result['_deleted']
            if 'error' in result:
                ctx = self.pending[orig['_id']]
                ctx.error = result['error']
            else:
                if isinstance(orig, dict):
                    if 'rev' in result:
                        orig['_rev'] = result['rev'] # if batch=ok we won't get one                    
                    doc = defaults.types.doc(orig.items())
                    orig_idx = self._originals.index(orig)
                    self._originals[orig_idx] = doc
                    self.resolved[result['id']] = doc
                    del self.pending[result['id']]

    def _reflect_bulk_get(self, resp):
        for doc in iter(resp):
            ctx = self.pending[doc['_id']]
            ctx.server_doc = doc

    def overwrite(self, callback=None):
        """Attempt to replace the server's copy of the docs with those in .pending
        
        This is a brute-force method of resolving conflicts. The database is queried
        for the _revs of all the docs with pending conflicts. The local copies of the
        docs are then updated with the up-to-date _revs and a bulk-save is attempted.
        
        Returns:
            self (after updating the pending and resolved dicts)
        """
        
        def dead_history(local_doc, server_doc):
            local_doc['_rev'] = server_doc['_rev']
            return local_doc
        return self._begin_bulk_update(dead_history, callback=callback, include_docs=False)

    def _post(self, docs, process=None, callback=None, **options):
        to_post = []
        for doc in docs:
            if isinstance(doc, dict):
                to_post.append(doc)
            elif hasattr(doc, 'items'):
                to_post.append(dict(doc.items()))
            else:
                raise TypeError('expected dict, got %s' % type(doc))
    
        content = dict(docs=to_post)
        content.update(options)
        return self._db.resource.post_json('_bulk_docs', body=content, process=process, callback=callback, **options)

    def _begin_bulk_update(self, merge, callback=None, include_docs=True):
        def postproc(server_docs, status):
            # update the conflict ctx dicts with the newly fetched copy of each 
            # conflicted document
            self._reflect_bulk_get(server_docs)

            # then run the merge fn
            updated = []
            for c_id, ctx in self.pending.iteritems():
                winner = merge(ctx.doc, ctx.server_doc)
                updated.append(winner)
            updated = [u for u in updated if u]
            
            # then do an update and return ourselves (after updating the conflicts dict)
            if updated:
                def merge_results(bulk_docs_response, status):
                    self._reflect_bulk_post(bulk_docs_response, updated)
                    if callback:
                        callback(self, status)
                    else:
                        return self, status
                merge_cb = merge_proc = None
                if callback: merge_cb=merge_results
                else: merge_proc=merge_results
                self._post(updated, process=merge_proc, callback=merge_cb)

            return self, status
            
        cb = proc = None        
        if callback: cb=postproc
        else: proc=postproc
        return self._db._bulk_get(self.pending.keys(), process=proc, callback=cb, include_docs=include_docs)

    def resolve(self, merge, callback=None):
        """Attempt to merge the local and server versions of docs with pending conflicts.
        
        Allows a user-provided function to decide how to resolve each conflict and what
        (if anything) ought to be written to the server.
        
        Args:
            merge (function w/ signature ƒ(local_doc, server_doc)): This function should
            create a new version of the doc incorporating the proper elements of both
            copies. If a dict-like object is returned, an attempt will be made to write
            it to the database. If `None` is returned, the document will be skipped in
            the write attempt and remains ‘pending’.
            
        Returns:
            self (after updating the pending and resolved dicts)
        
        """
        self._begin_bulk_update(merge, callback)
    

