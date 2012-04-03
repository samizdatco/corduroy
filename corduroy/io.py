# encoding: utf-8
"""
corduroy.io

Synchromesh HTTP Bit-slinging Mayhem.
"""

import sys, os, re
import urllib
import logging
import mimetypes
from datetime import timedelta
from base64 import b64encode
from urlparse import urlsplit, urlunsplit, urlparse, urlunparse
from base64 import b64encode
from inspect import getouterframes, currentframe
from pdb import set_trace as tron

from .atoms import *
from .exceptions import *
from .config import defaults, json
from . import __version__ as VERSION

_logger = logging.getLogger('corduroy')
def log(*msg):
    _logger.info((u" ".join([unicode(s) for s in msg])).encode('utf-8'))

def guess_mime(filename):
    return ';'.join(filter(None, mimetypes.guess_type(filename)) or 'application/octet-stream')

def serialize_doc(doc, _encode=True):
    body = content_type = None
    _att = odict()
    for fn, info in doc.get('_attachments',{}).iteritems():
        if hasattr(info, 'read') and info.read:
            _att[fn] = dict(content_type=guess_mime(fn), data=b64encode(info.read()))
        elif hasattr(info.get('data'), 'read') and info.get('data').read:
            data = info['data']
            _att[fn] = dict(content_type=info.get('content_type',None) or guess_mime(fn),
                            data=b64encode(data.read()))
        else:
            _att[fn] = info

    updated_doc = doc.copy()
    if '_attachments' in updated_doc:
        updated_doc['_attachments'] = _att
    if _encode:
        return json.encode(updated_doc).encode('utf-8')
    return updated_doc

def serialize_bulk(body):
    body['docs'] = [serialize_doc(d, _encode=False) for d in body['docs']]
    return json.encode(body).encode('utf-8')    
    
    
def denormalize_url(url, creds):
    if not creds:
        return url
    parts = urlsplit(url)
    netloc = '%s:%s@%s'%(creds[0], creds[1], parts.netloc)
    parts = list(parts)
    parts[1] = netloc
    return urlunsplit(tuple(parts))
    

def normalize_url(url):
    """Extract authentication credentials from the given URL and prepend the default host if omitted. """
    if url is None:
        url = '%s:%i'%(defaults.host, defaults.port)

    elif not url.startswith('http'):
        if '.' in url:
            # presume this is a domain name
            url = 'http://%s' % url
        else:
            # presume this is a subdir under the default server address
            url = "%s:%i/%s"%(defaults.host.rstrip('/'), defaults.port, url.lstrip('/'))

    parts = urlsplit(url)
    if '@' in parts.netloc:
        creds, netloc = parts.netloc.split('@')
        credentials = tuple(creds.split(':'))
        parts = list(parts)
        parts[1] = netloc
    else:
        parts = list(parts)
        credentials = None

    if ':' not in parts[1] and defaults.port!=80:
        parts[1] += ':%i'%defaults.port
    elif parts[1].endswith(':80'):
        parts[1] = parts[1][:-3]


    return urlunsplit(tuple(parts)), credentials

def quote(string, safe=''):
    if isinstance(string, unicode):
        string = string.encode('utf-8')
    return urllib.quote(string, safe)

def urlencode(data):
    if isinstance(data, dict):
        data = data.items()
    params = []
    for name, value in data:
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        params.append((name, value))
    return urllib.urlencode(params)

def urljoin(base, *path, **query):
    """Assemble a uri based on a base, any number of path segments, and query
    string parameters.

    >>> urljoin('http://example.org', '_all_dbs')
    'http://example.org/_all_dbs'

    A trailing slash on the uri base is handled gracefully:

    >>> urljoin('http://example.org/', '_all_dbs')
    'http://example.org/_all_dbs'

    And multiple positional arguments become path parts:

    >>> urljoin('http://example.org/', 'foo', 'bar')
    'http://example.org/foo/bar'

    All slashes within a path part are escaped:

    >>> urljoin('http://example.org/', 'foo/bar')
    'http://example.org/foo%2Fbar'
    >>> urljoin('http://example.org/', 'foo', '/bar/')
    'http://example.org/foo/%2Fbar%2F'

    >>> urljoin('http://example.org/', None) #doctest:+IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    TypeError: argument 2 to map() must support iteration
    """
    if base and base.endswith('/'):
        base = base[:-1]
    retval = [base]

    # build the path
    path = '/'.join([''] + [quote(s) for s in path])
    if path:
        retval.append(path)

    # build the query string
    params = []
    for name, value in query.items():
        if type(value) in (list, tuple):
            params.extend([(name, i) for i in value if i is not None])
        elif value is not None:
            if value is True:
                value = 'true'
            elif value is False:
                value = 'false'
            params.append((name, value))
    if params:
        retval.extend(['?', urlencode(params)])

    return ''.join(retval)

    
def is_relaxed():
    for _, filename, _, function_name, _, _ in getouterframes(currentframe())[:30]:
        if 'tornado/gen.py' in filename:
            return True # Task()...
    return False


class Resource(object):
    def __init__(self, url, headers=None, auth=None):
        self.url, credentials = normalize_url(url)
        self.credentials = auth if auth else credentials
        self.headers = headers or {}
        self.io = IO()

    def __call__(self, *path):
        obj = type(self)(urljoin(self.url, *path), auth=self.credentials)
        obj.headers = self.headers.copy()
        return obj

    @property
    def auth_url(self):
        return denormalize_url(self.url, self.credentials)
        
    def delete(self, path=None, headers=None, **params):
        return self._request('DELETE', path, headers=headers, **params)

    def get(self, path=None, headers=None, **params):
        return self._request('GET', path, headers=headers, **params)

    def head(self, path=None, headers=None, **params):
        return self._request('HEAD', path, headers=headers, **params)

    def post(self, path=None, body=None, headers=None, **params):
        return self._request('POST', path, body=body, headers=headers,
                            **params)

    def put(self, path=None, body=None, headers=None, **params):
        return self._request('PUT', path, body=body, headers=headers, **params)

    def delete_json(self, path=None, headers=None, **params):
        return self._request_json('DELETE', path, headers=headers, **params)

    def get_json(self, path=None, headers=None, **params):
        return self._request_json('GET', path, headers=headers, **params)

    def post_json(self, path=None, body=None, headers=None, **params):
        return self._request_json('POST', path, body=body, headers=headers,
                                **params)

    def put_json(self, path=None, body=None, headers=None, **params):
        return self._request_json('PUT', path, body=body, headers=headers,
                                **params)


    def _request(self, method, path=None, body=None, headers=None, asjson=False, 
                       process=None, callback=None, **params):
        
        method = method.upper()
        
        all_headers = self.headers.copy()
        all_headers.update(headers or {})

        if headers is None:
            headers = {}
        else:
            headers = all_headers
        headers.setdefault('Accept', 'application/json')
        headers['User-Agent'] = 'Corduroy/%s' % VERSION

        if path is not None:
            url = urljoin(self.url, path, **params)
        else:
            url = urljoin(self.url, **params)

        # if it seems dict- or list-like, check for file objects in the _attachments field
        # and treat those separately. otherwise json encode it and hope for the best
        if body is not None and not isinstance(body, basestring):
            if not hasattr(body, 'read') or not body.read:                
                if isinstance(body.get('docs'), (list,tuple)):
                    # import pdb; pdb.set_trace()
                    body = serialize_bulk(body)
                else:
                    body = serialize_doc(body)
                headers['Content-Type']='application/json'

        if body is None:
            headers.setdefault('Content-Length', '0')
        elif isinstance(body, basestring):
            headers.setdefault('Content-Length', str(len(body)))
        elif hasattr(body, 'read') and body.read:
            body = body.read()
            
        # path_query = urlunsplit(('', '') + urlsplit(url)[2:4] + ('',))
        req = dict(method=method, headers=headers, url=url)
        if body is not None: 
            req['data'] = body
        elif method in ('POST','PUT'):
            req['data'] = ''
        if self.credentials:
            req['auth'] = self.credentials

        # if there's a callback, try to use one of the async clients
        if callback and hasattr(callback,'__call__'):
            def response_ready(data, status):
                if hasattr(process,'__call__'):
                    data, status = process(data, status)
                callback(data, status)
            return self.io.fetch(callback=response_ready, **req)

        # otherwise use the blocking client
        return self.io.fetch(process=process, **req)


    def _request_json(self, method, path=None, body=None, headers=None, callback=None, process=None, **params):
        def preprocess(data, status):
            if data and status['headers'] and 'application/json' in status.headers.get('Content-Type'):
                try:
                    data = json.decode(data)
                except TypeError:
                    pass # we didn't get a response at all
                except ValueError:
                    pass # it wasn't valid json
            if hasattr(process,'__call__'):
                data, status = process(data, status) 
            return data, status

        # for async calls, return value is a Request object
        # for non-async calls, return value is the data if no exception was raised
        return self._request(method, path, body=body, headers=headers, 
                             process=preprocess, callback=callback, **params)

def validate_response(resp, bail_on_error=False):
    code = data = None
    try:
        code = resp.code
        data = resp.body
    except:
        code = resp.status_code
        data = resp.content
    status = Status(code, headers=resp.headers)

    m = re.search(r'charset=([^; ]+)', resp.headers.get('content-type',''))
    if m:
        data = data.decode(m.group(1))        

    # Handle errors
    if code >= 400:
        status.ok = False
        status.response = data
        # try to get info out of the response then pass it along (as a dict if possible)
        exc_info = ''
        if data is not None:
            status.response = data
            if 'application/json' in resp.headers['content-type'] and data:
                status.response = json.decode(data)
                exc_info = data.strip()
            elif isinstance(data, basestring):
                exc_info = data
        data = None
        
        exceptions={401:Unauthorized, 404:NotFound, 409:Conflict, 412:PreconditionFailed}
        
        if status.code in exceptions:
            status.error = exceptions[code]
            status.exception = exceptions[code](exc_info)
        elif code >= 500:
            status.error = ServerError
            status.exception = ServerError(exc_info)
        else:
            status.error = HTTPError
            status.exception = HTTPError(exc_info)

    if bail_on_error and not status.ok:
        raise status.exception
    return data, status

class IO(object):
    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(IO, cls).__new__(cls)
            # wait until first invocation to try and suss out which async library to use.
            cls._instance._client = None
        return cls._instance    

            
    def fetch(self, method, url, data=None, headers=None, auth=None, process=None, callback=None, _i_n_t_e_r_c_e_p_t_=False):
        self._client = self._client or TornadoClient() or RequestsClient()
        if not self._client:
            raise RuntimeError('Neither tornado nor requests is available.')
            
        if is_relaxed() and not callback:
            # asynchronous fetch using the @relax decorator
            def just_the_facts(data, status):
                if process:
                    data, status = process(data, status)
                if status.ok:
                    return data, None
                else:
                    raise status.exception
            return self._client.async.gen.Task(self._client.fetch, method, url, data, headers, auth, process=just_the_facts)
        else:
            return self._client.fetch(method=method, url=url, data=data, headers=headers, auth=auth, process=process, callback=callback)
            

class RequestsClient(object):
    def __init__(self):
        self._ready = False
        try:
            import requests
            self.blocking = adict(client=requests, request=requests.request)
            self._ready = True
            from requests import async as client

            # note that this is monkeypatch city and replaces the system's
            # socket class (among others). once gevent has taken over, tornado
            # won't behave properly since it's getting the patched i/o objects
            # instead of the ‘real’ ones
            import gevent as gev
            self.async = adict(client=client, request=client.request,
                            spawn=gev.spawn, sleep=gev.sleep, 
                            timer=gev.core.timer)
            log('Using requests.async (gevent)')
        except ImportError:
            pass

    def __len__(self):
        return 1 if self._ready else 0

    def fetch(self, method, url, data=None, headers=None, auth=None, process=None, callback=None):
        req = dict(method=method, url=url, headers=headers, data=data,
                   auth=auth)
        if hasattr(callback, '__call__'):
            log(u"⌁ %4s %s"%(method, url))
        
            def process_gevent_resp(resp):
                data, status = validate_response(resp)
                if process:
                    data, status = process(data, status)
                if status:
                    callback(data, status)
                else:
                    callback(data)
            req['hooks']=dict(response=process_gevent_resp)
            async_req = self.async.request(**req)
            self.async.client.send(async_req)
            return async_req
        else:
            log(u"✓ %4s %s"%(req['method'],req['url']))
            resp = self.blocking.client.request(**req)
            data, status = validate_response(resp, bail_on_error=True)
            if process:
                data, status = process(data, status)
            return data
            
    def feed(self, endpoint, listener):
        an_hour = 60*60
        
        def listen():        
            def iter_response(resp):
                for ln in resp.iter_lines():
                    listener.response(ln)
            req = self.async.request(method="GET", url=endpoint, timeout=an_hour, prefetch=False,
                                     hooks=dict(response=iter_response))
            if listener.auth:
                req.auth_username, self.req.auth_password = listener.auth
            req.send()
        
        client = self.async.spawn(listen)
        self.async.sleep(0)
        return client
        
        
    def timeout(self, secs, callback):
        return self.async.timer(secs, callback, object())
        
    def close(self):
        self.async.client.kill(block=False)

class TornadoClient(object):
    def __init__(self):
        self._ready = False
        if any(m for m in sys.modules.keys() if m.startswith('tornado')):
            try:
                from tornado import httpclient, ioloop, gen
                self.blocking = adict(client=httpclient.HTTPClient(), request=httpclient.HTTPRequest, error=httpclient.HTTPError)
                self.async = adict(client=httpclient.AsyncHTTPClient(force_instance=True), request=httpclient.HTTPRequest, 
                                           loop=ioloop.IOLoop.instance(), gen=gen)
                self._ready = True

                # prefer the libcurl-based client because it's fast (even though pycurl's
                # very literal transcription of the c ‘api’ doesn't fill me with confidence
                # about what's happening in ffi-land...)
                log("Using httpclient (tornado)")
                import pycurl
                httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
                log("... with the pycurl backend")
            except ImportError:
                pass

    def __len__(self):
        return 1 if self._ready else 0

    def fetch(self, method, url, data=None, headers=None, auth=None, process=None, callback=None):
        if 'Content-Length' in headers:
            del headers['Content-Length'] # tornado mangles this if you include it. do they all?
    
        req = adict(method=method, url=url, headers=headers, allow_nonstandard_methods=True)
        if auth:
            req.auth_username, req.auth_password = auth
        if data is not None:
            req.body = data
        req.request_timeout = defaults.http.timeout

        if hasattr(callback, '__call__'):
            log(u"⌁ %4s %s"%(method, url))
            async_req = self.async.request(**req)
        
            def process_tornado_resp(resp):
                data, status = validate_response(resp)
                if process:
                    data, status = process(data, status)
                if status:
                    callback(data, status)
                else:
                    callback(data)                
            self.async.client.fetch(async_req, process_tornado_resp)
            return async_req
        else: 
            log(u"✓ %4s %s"%(req['method'],req['url']))
            sync_req = self.blocking.request(**req)
            try:
                resp = self.blocking.client.fetch(sync_req)
            except self.blocking.error, e:
                resp = e.response
            
            data, status = validate_response(resp, bail_on_error=True)
            if process:
                data, status = process(data, status)
            return data
    
    def feed(self, endpoint, listener):
        def buffer_response(resp, buf=''):
            buf += resp
            for ln in buf.splitlines(True):
                if not ln.endswith('\n'):
                    buf = ln
                    break
                listener._response(ln)
            else:
                buf = ''

        an_hour = 60*60        
        req = self.async.request(endpoint, streaming_callback=buffer_response,
                               connect_timeout=an_hour, request_timeout=an_hour)
        if listener.auth:
            req.auth_username, self.req.auth_password = listener.auth
        self.async.client.fetch(req, listener._closed)
        
    def timeout(self, secs, callback):
        return self.async.loop.add_timeout(timedelta(seconds=secs), callback)

    def close(self):
        self.async.client.close()

class ChangesFeed(object):
    """Persistent listener to a Database's `_changes` endpoint
    
    Attributes:
        callback (function w/ signature ƒ(seq, changes)): the user callback to which incoming 
        changesets will be passed.

        latency (float): the minimum time (in seconds) between invocations of the user callback.
    """
    def __init__(self, database, filter=None, heartbeat=60, since=0, latency=0.666, callback=None, **options):
        self.latency = latency # in seconds
        self.callback = callback
        self._timeout = None
        self._client = None
        self._changes = []
        if heartbeat is not None:
            heartbeat *= 1000
                
        rsrc = database.resource('_changes')
        self.url = '%s/_changes'%rsrc.url
        self.auth = rsrc.credentials
        self.listening = False
        self.seq = since
        params = dict(feed='continuous', heartbeat=heartbeat, filter=filter)
        params.update(options)
        if not filter: del params['filter']
        if hasattr(self.callback,'__call__'):
            if heartbeat is None: del params['heartbeat']
        self.query = urljoin('',**params)
        self.listen()
    
    def stop(self):
        """Close the connection to the server"""
        if not self.listening:
            print "already stopped"
            return

        # print "shut it down"
        self._client.close()
        self.listening=False

    def listen(self):
        """Open a new connection"""
        if self.listening:
            print "already listening"
            return
        self.listening=True
        self._client = self._client or TornadoClient() or RequestsClient()
        # print "listen",self._client
        if not self._client:
            raise RuntimeError('Neither tornado nor requests is available.')


        endpoint = '%s%s&since=%i'%(self.url, self.query, self.seq)
        self._client.feed(endpoint, self)

    def _gevent_response(self, resp):
        for ln in resp.iter_lines():
            self._readline(ln)
    
    def _response(self, ln):
        ln = ln.strip()
        if not ln: return

        changed = json.decode(ln)
        if changed:
            self.seq = changed.get('seq', self.seq)
            self._changes.append(changed)
            if not self._timeout:
                self._timeout = self._client.timeout(self.latency, self._hand_off)
                
    def _hand_off(self, _gevent_id=None):
        self.callback(self.seq, self._changes)
        self._timeout = None
        self._changes = []
        
    def _closed(self, resp):
        self.listening=False
        self._client=None
        # self._closed(resp)
        # print "connection closed itself",resp


