#!../../env/bin/python
# encoding: utf-8
"""
spanx.py

it ain’t sphinx

Created by Christian Swinehart on 2012-03-04.
Copyright (c) 2012 Samizdat Drafting Co. All rights reserved.
"""

from __future__ import with_statement
import sys
import os
import re
import pystache
import inspect
from cgi import escape
from textwrap import dedent
from operator import itemgetter

py_root = os.path.dirname(os.path.abspath(__file__))
doc_root = os.path.abspath('%s/..'%py_root)
sys.path.append(py_root)
sys.path.append(os.path.abspath('%s/../..'%py_root))
sys.path.append(os.path.abspath('%s/tmpl'%py_root))
from examples import examples, fmt

from corduroy import *
from corduroy.atoms import *
from corduroy.io import *
from corduroy.exceptions import *
from pygments.lexers import PythonLexer

def redent(txt):
    if u'\n' in txt:
        first,rest = txt.split(u'\n',1)
        return u'"%s\n%s"'%(first,dedent(rest))
    return txt
    

re_section = re.compile(r'^ *((?:[A-Z][a-z]+ ?)+): *$')
def meth_info(docstring):
    docstr = redent(docstring).strip('"')

    sections = odict(frontmatter=[])
    sect = u'frontmatter'
    for line in docstr.split('\n'):
        m = re_section.search(line)
        if m:
            sect = m.group(1)
            sections[sect] = []
        else:
            sections[sect].append(line)
    
    meth = dict(about=[], sections=[])
    for sect, lines in sections.items():
        if sect=='frontmatter':
            meth['about'] = [dict(par=graf.strip()) for graf in re.split(r'\n[ ]*\n', "\n".join(lines))]
        else:
            section = dict(name=sect, params=[])
            for line in re.split(r'\n[ ]*\n', dedent("\n".join(lines))):
                m = re.match(r'(.*?) \((.*?)\): (.*)$', line, re.S)
                if m:
                    param, typ, doc = m.groups()
                    # section['params'].append({"param":param, "type":typ, "doc":doc.strip()})
                    # doc = markdown(doc)
                    # blob = "<span class='param'>%s</span> (<em>%s</em>): %s"%(param, typ, doc)
                    section['params'].append({"doc":"<li class='dedent'><span class='param'>%s</span> (<em>%s</em>): %s</li>"%(param, typ, doc)})
                    # if 'stale' in param:
                    #     tron()
                    # section['params'].append({"doc":"<li>%s</li>"%markdown(blob)})
                else:
                    section['params'].append({"doc":"<li>%s</li>"%line.strip()})
            meth['sections'].append(section)
                
    return meth

def meth_signature(fn):
    try:
        argspec = inspect.getargspec(fn)
        if argspec[0]:
            del argspec[0][0]
        
        args = []
        for a in (inspect.formatargspec(*argspec)[1:-1]).split(', '):
            bits = a.split('=')
            if len(bits)==2:
                args.append('%s=<code>%s</code>'%(bits[0],bits[1]))
            else:
                args.append(a)
            
        args = '(%s)'%(", ".join(args))

    except TypeError:
        args = u'<span class="no-args">‹property›</span>'
    
    return args
    
def spanx(cls):
    clsdoc = dict(classname=cls.__name__, intro=[], methods=[])
    if cls.__name__=='relax':
        clsdoc['classname'] = u"@relax"

    if cls.__doc__:
        raw_intro = redent(cls.__doc__.decode('utf-8')).strip('"')
        clsdoc['intro'] = [dict(graf=graf.strip()) for graf in re.split(r'\n[ ]*\n', raw_intro)]
        
        info = meth_info(raw_intro)
        clsdoc['intro'] = [dict(graf=d['par']) for d in info['about']]
        if info['sections']:
            clsdoc['sections'] = info['sections']
        
    for nm, fn in cls.__dict__.items():
        if nm.startswith('_') and nm!='__init__': continue
        if not fn.__doc__: continue
        docstring = fn.__doc__.decode('utf-8')
        args = meth_signature(fn)

        meth_documentation = dict(name=nm, args=args, hash="%s.%s"%(cls.__name__, nm))
        meth_documentation.update(meth_info(docstring))
        clsdoc['methods'].append(meth_documentation)

    clsdoc['methods'] = sorted(clsdoc['methods'], key=itemgetter('name'))
    
    return clsdoc
    

def makemain(paths):
    def _output(text):
        return u'<div class="output"><pre>%s</pre></div>'%escape(text)

    tmpl = file('%s/tmpl/index.html'%py_root).read().decode('utf8')
    examples['output'] = _output
    examples.update(paths)
    html = pystache.render(tmpl, examples)
    fn = 'index.html' if paths['root'] else 'readme.html'
    with file('%s/%s'%(doc_root,fn),'w') as f:
        f.write(html.encode('utf8'))

def makeguide(paths):
    def _output(text):
        return u'<div class="output"><pre>%s</pre></div>'%escape(text)

    tmpl = file('%s/tmpl/guide.html'%py_root).read().decode('utf8')
    examples['output'] = _output
    examples.update(paths)
    html = pystache.render(tmpl, examples)
    
    fn = 'guide/index.html' if paths['root'] else 'guide.html'
    with file('%s/%s'%(doc_root,fn),'w') as f:
        f.write(html.encode('utf8'))
    
def makeref(paths):
    pkg = odict([('CouchDB',[Couch, Database]),
                 ('Atoms',[View, Row, Document, Status]),
                 ('IO',[ChangesFeed]),
                 ('Exceptions',[ConflictResolution]),
                 ('_IO',[relax]),
                 # ('Exceptions',[HTTPError, NotFound, Conflict, PreconditionFailed, ServerError]),
                 
               ])
    
    modules = []
    for mod, classes in pkg.items():
        module = dict(module=mod, classes=[])
        modules.append(module)
        for cls in classes:
            module['classes'].append(spanx(cls))
            module['classes'][-1]['module'] = mod.lower()
    info = dict(modules=modules)
    info.update(paths)
    
    tmpl = file('%s/tmpl/reference.html'%py_root).read().decode('utf8')
    html = pystache.render(tmpl, info)
    def inlinify(m):
        hunks = m.group(1).split(' ')
        return " ".join(["<code class=\"inline\">%s</code>"%h for h in hunks])
    html = re.sub(r'``(.*?)``', inlinify, html)
    html = re.sub(r'`(.*?)`', r'<code>\1</code>', html)
    fn = 'ref/index.html' if paths['root'] else 'reference.html'
    with file('%s/%s'%(doc_root,fn),'w') as f:
        f.write(html.encode('utf8'))

if __name__ == '__main__':
    roots = dict(guide="/corduroy/guide/", readme="/corduroy", ref="/corduroy/ref/", root="/corduroy/")
    if len(sys.argv)>1 and sys.argv[1]=='standalone':
        roots = dict(guide="guide.html", readme="readme.html", ref="reference.html", root="")
    else:
        for pth in ['ref','guide']:
            abspth = "/".join([doc_root,pth])
            if not os.path.exists(abspth): os.makedirs(abspth)
        
    makeguide(roots)
    makeref(roots)
    makemain(roots)
