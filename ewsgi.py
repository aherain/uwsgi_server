#-- coding: utf-8 -- 

import urllib

import urllib.parse
import http.cookies
import os
import os.path
import sys
import threading
import json
import re
import copy

from collections import namedtuple
from wsgiref.util import FileWrapper

import mimetypes
import cgi

httpcodes = [
    ("CONTINUE", 100),
    ("SWITCHING_PROTOCOLS", 101),
    ("PROCESSING", 102),
    ("OK", 200),
    ("CREATED", 201),
    ("ACCEPTED", 202),
    ("NON_AUTHORITATIVE_INFORMATION", 203),
    ("NO_CONTENT", 204),
    ("RESET_CONTENT", 205),
    ("PARTIAL_CONTENT", 206),
    ("MULTI_STATUS", 207),
    ("IM_USED", 226),
    ("MULTIPLE_CHOICES", 300),
    ("MOVED_PERMANENTLY", 301),
    ("FOUND", 302),
    ("SEE_OTHER", 303),
    ("NOT_MODIFIED", 304),
    ("USE_PROXY", 305),
    ("TEMPORARY_REDIRECT", 307),
    ("BAD_REQUEST", 400),
    ("UNAUTHORIZED", 401),
    ("PAYMENT_REQUIRED", 402),
    ("FORBIDDEN", 403),
    ("NOT_FOUND", 404),
    ("METHOD_NOT_ALLOWED", 405),
    ("NOT_ACCEPTABLE", 406),
    ("PROXY_AUTHENTICATION_REQUIRED", 407),
    ("REQUEST_TIMEOUT", 408),
    ("CONFLICT", 409),
    ("GONE", 410),
    ("LENGTH_REQUIRED", 411),
    ("PRECONDITION_FAILED", 412),
    ("REQUEST_ENTITY_TOO_LARGE", 413),
    ("REQUEST_URI_TOO_LONG", 414),
    ("UNSUPPORTED_MEDIA_TYPE", 415),
    ("REQUESTED_RANGE_NOT_SATISFIABLE", 416),
    ("EXPECTATION_FAILED", 417),
    ("UNPROCESSABLE_ENTITY", 422),
    ("LOCKED", 423),
    ("FAILED_DEPENDENCY", 424),
    ("UPGRADE_REQUIRED", 426),
    ("PRECONDITION_REQUIRED", 428),
    ("TOO_MANY_REQUESTS", 429),
    ("REQUEST_HEADER_FIELDS_TOO_LARGE", 431),
    ("INTERNAL_SERVER_ERROR", 500),
    ("NOT_IMPLEMENTED", 501),
    ("BAD_GATEWAY", 502),
    ("SERVICE_UNAVAILABLE", 503),
    ("GATEWAY_TIMEOUT", 504),
    ("HTTP_VERSION_NOT_SUPPORTED", 505),
    ("INSUFFICIENT_STORAGE", 507),
    ("NOT_EXTENDED", 510),
    ("NETWORK_AUTHENTICATION_REQUIRED", 511),
]

httpcodes_s2i = dict(httpcodes)
httpcodes_i2s = dict([(i, s) for s, i in httpcodes])


class HttpResponse(object):
    
    def __init__(self, status, reason, headers, body):

        self.exc_info = None

        self.status = status if status else httpcodes_s2i[reason]
        self.reason = reason if reason else httpcodes_i2s[status]
        self.headers = list(headers.items()) if type(headers) == dict else headers
        
        if body is None:
            body = b''

        if type(body) == str:
            body = body.encode('utf-8')
        elif type(body) == bytes:
            body = body
        else :
            body = json.dumps(body, ensure_ascii=False, default=str).encode('utf-8')
        
        self.body = body

        return

    def headstruct(self):
        return ('%s %s' % (self.status, self.reason), self.headers, self.exc_info)
        
        

class HttpOK(HttpResponse):
    def __init__(self, headers=[], body=''):
        super().__init__(200, None, headers, body)
        return


class HttpFile(HttpResponse):
    
    def __init__( self, filepath ):
    
        fp = open( filepath, 'rb' )
        mtype = mimetypes.guess_type( filepath )[0]
        
        headers = []
        
        if mtype :
            headers.append( ('Content-Type', mtype) )
        
        super().__init__(200, None, headers, body)
        
        return
        

class HttpBadRequest(HttpResponse):
    def __init__(self, headers=[], body=''):
        super().__init__(400, None, headers, body)
        return


class HttpNotFound(HttpResponse):
    def __init__(self, headers=[], body=''):
        super().__init__(404, None, headers, body)
        return


class HttpForbidden(HttpResponse):
    def __init__(self, headers=[], body=''):
        super().__init__(403, None, headers, body)
        return


class HttpRedirect(HttpResponse):
    def __init__(self, redirect_url):
        self.redirect_url = redirect_url
        headers = {'Location': redirect_url}
        super().__init__(302, None, headers, '')
        return


class HttpXRedirect(HttpResponse):
    def __init__(self, redirect_url):
        self.redirect_url = redirect_url
        headers = {'X-Accel-Redirect': redirect_url}
        super().__init__(301, None, headers, '')
        return


class HttpInternalServerError(HttpResponse):
    def __init__(self, exc_info=None, body=''):
        self.exc_info = sys.exc_info() if exc_info == None else exc_info
        super().__init__(500, None, [], body)
        return



import collections
class AttrDict(collections.UserDict):

    def __missing__(self, key):
        return ''
    
    def __getattr__( self, key ):
        return self[key]
    
    
    
class WsgiServer(object):
    
    Response = HttpResponse
    Redirect = HttpRedirect
    XRedirect = HttpXRedirect
    File = HttpFile
    
    OK = HttpOK
    BadRequest = HttpBadRequest
    NotFound = HttpNotFound
    Forbidden = HttpForbidden
    InternalServerError = HttpInternalServerError
    
    def __init__( self ):
        
        self.rules = [ (m[3:].replace('__', '/'), m) for m in dir(self) if m.startswith('url__') ]
        self.rules = dict(self.rules)
        
        self.re_rules = []
        
        return
        
    def __call__( self, environ, start_response ):
        return copy.copy(self).process( environ, start_response )
        
    def process( self, environ, start_response ):
        
        w, args = self.http_entry( environ )
        
        resp = w(*args)
        start_response( *resp.headstruct() )
        
        return resp.body
    
    def http_entry( self, environ ):
        
        self.env = AttrDict(environ)
        self.host = environ['HTTP_HOST']
        self.path = environ['PATH_INFO']
        self.user_agent = environ.get('HTTP_USER_AGENT','')
        
        self.session = AttrDict({})
        self.cookie = AttrDict({})
        
        try :
            self.session = AttrDict(json.loads( environ['USER_SESSION'] ))
        except :
            pass
        
        try :
            c = http.cookies.SimpleCookie()
            c.load( environ['HTTP_COOKIE'] )
            c = [ (ci.key.lower(), ci.value) for ci in c.values() ]
            self.cookie = AttrDict(c)
        except :
            pass
        
        
        args = {}
        
        path = environ['PATH_INFO']
        wi = self.rules.get(path)
        w = None
        
        if wi == None :
            
            for resurl in self.re_rules :
                if path.startswith(resurl):
                    w = getattr( self, 'res'+resurl.replace('/','__'), None )
                    if w != None :
                        args['_res'] = path[len(resurl):]
                    break
        
        else :
            
            w = getattr( self, wi, None )
        
        
        if w :
            
            qs = environ['QUERY_STRING'].split('&')
            qs = [ x.split('=',1) for x in qs if x ]
            qs = [ (k, urllib.parse.unquote_plus(v)) for k, v in qs if not k.startswith('_') ]
            qs = dict(qs)
            
            args.update(qs)
            
            if environ['REQUEST_METHOD'] == 'POST' :
                
                ctype, pdict = cgi.parse_header( environ.get('HTTP_CONTENT_TYPE', environ.get('CONTENT_TYPE','')) )
                
                if ctype.startswith('application/x-www-form-urlencoded') :
                
                    pd = environ['wsgi.input'].read().decode('utf-8')
                    pd = pd.split('&')
                    pd = [ x.split('=',1) for x in pd if x ]
                    pd = [ (k, urllib.parse.unquote_plus(v)) for k, v in pd ]
                    pd = dict(pd)
                
                    args.update(pd)
                
                elif ctype.startswith('multipart/form-data') :
                    
                    if 'UPLOAD_PARAMS' in environ :
                        args.update( json.loads( environ['UPLOAD_PARAMS'] ) )
                    else :
                        if type(pdict['boundary']) == str :
                            pdict['boundary'] = pdict['boundary'].encode('ascii')
                        
                        pd = cgi.parse_multipart(environ['wsgi.input'], pdict)
                        args.update(pd)
                
                elif ctype.startswith('application/json') or ctype.startswith('text/plain') :
                    
                    pd = environ['wsgi.input'].read().decode('utf-8')
                    pd = json.loads( pd )
                    
                    if type(pd) == dict :
                        args.update( pd )
                    else :
                        args['_json_arg'] = pd
                    
            return ( self.http_cgi, (w, args, c) )

        return ( self.http_notfound, () )

    def http_cgi( self, work, args, cookie ):
        
        try :
            for k, p in work.__annotations__.items():
                if type(p) == type and k in args :
                    try :
                        args[k] = p(args[k])
                    except :
                        raise AssertionError('不符合条件的参数错误')
                    
            resp = work(**args)
        except Exception as e:
            return self.http_exception( e )

        if not isinstance( resp, HttpResponse ):
            resp = self.OK( [], resp )
            
        return resp
    
    def http_exception( self, e ):
        
        import traceback
        traceback.print_exc()
        
        if isinstance(e, TypeError) and e.args[0].startswith('url__') :
            return self.BadRequest([],'不符合要求的参数错误')
        
        if isinstance(e, AssertionError) :
            return self.BadRequest([],e.args[0])
        
        return self.InternalServerError( e )
        
    def http_notfound( self ):
        return self.NotFound()
    
    def uri( self, **kwargs ):
        
        a = self.env.args.copy()
        a.update( kwargs )
        
        params = urllib.parse.urlencode(a).strip()
        
        if params :
            return self.env.path+'?'+params
        
        return self.env.path




# code message meaning
# -32700 Parse error Invalid JSON was received by the server.
# An error occurred on the server while parsing the JSON text.
# -32600 Invalid Request The JSON sent is not a valid Request object.
# -32601 Method not found The method does not exist / is not available.
# -32602 Invalid params Invalid method parameter(s).
# -32603 Internal error Internal JSON-RPC error.
# -32000 to -32099 Server error Reserved for implementation-defined server-errors.

class JrOK(HttpOK):
    def __init__(self, headers=[], result=None):
        headers = headers + [('Content-Type','text/html; charset=utf-8')]
        super().__init__(headers, {'status':200, 'result':result} )
        return

class JrBadRequest(HttpBadRequest):
    def __init__(self, headers=[], reason='bad request'):
        headers = headers + [('Content-Type','text/html; charset=utf-8')]
        super().__init__(headers,{'status':400, 'error':reason} )
        return

class JrNotFound(HttpNotFound):
    def __init__(self, headers=[], reason='not found'):
        headers = headers + [('Content-Type','text/html; charset=utf-8')]
        super().__init__(headers,{'status':404, 'error':reason} )
        return

class JrForbidden(HttpForbidden):
    def __init__(self, headers=[], reason='forbidden'):
        headers = headers + [('Content-Type','text/html; charset=utf-8')]
        super().__init__(headers, {'status':403, 'error':reason} )
        return

class JrInternalServerError(HttpInternalServerError):
    def __init__(self, exc_info=None, reason='internal error'):
        super().__init__(exc_info, {'status': 500, 'error':reason })
        return


class JrWsgiServer(WsgiServer):
    
    # Response = HttpResponse
    # Redirect = HttpRedirect
    # XRedirect = HttpXRedirect
    # File = HttpFile
    
    OK = JrOK
    BadRequest = JrBadRequest
    NotFound = JrNotFound
    Forbidden = JrForbidden
    InternalServerError = JrInternalServerError
    
    