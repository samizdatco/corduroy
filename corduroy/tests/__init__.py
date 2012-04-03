#!/usr/bin/env python
# encoding: utf-8
"""
corduroy.tests

Created by Christian Swinehart on 2012-02-25.
Copyright (c) 2012 Samizdat Drafting Co. All rights reserved.
"""

import unittest
from corduroy.tests import blocking, package, async

def suite():
    suite = unittest.TestSuite()
    suite.addTest(package.suite())
    suite.addTest(blocking.suite())
    suite.addTest(async.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
