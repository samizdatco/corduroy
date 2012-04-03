#!/usr/bin/env python
from distutils.core import setup
import codecs

README=codecs.open('README.rst', encoding='utf-8').read()
LICENSE=codecs.open('LICENSE', encoding='utf-8').read()
setup(
    name='corduroy',
    version='0.9.0',
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