#!/usr/bin/env python
from distutils.core import setup
import codecs
from corduroy import __version__ as VERSION

README=codecs.open('README.rst', encoding='utf-8').read()
LICENSE=codecs.open('LICENSE', encoding='utf-8').read()
setup(
    name='corduroy',
    version=VERSION,
    author='Christian Swinehart',
    author_email='drafting@samizdat.cc',
    packages=['corduroy', 'corduroy.tests'],
    url='http://samizdat.cc/corduroy',
    license=LICENSE,
    description='An asynchronous CouchDB client library',
    long_description=README,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)