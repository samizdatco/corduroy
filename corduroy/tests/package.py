# -*- coding: utf-8 -*-
"""
corduroy.tests.package
"""
import unittest
import corduroy

class PackageTestCase(unittest.TestCase):

    def test_exports(self):
        expected = set([
        'Couch', 'Database', 'Document', 'relax', 'defaults', 'HTTPError', 'Conflict', 
        'NotFound', 'PreconditionFailed', 'ServerError', 'Unauthorized'
        ])
        exported = set(e for e in dir(corduroy) if not e.startswith('_'))
        self.assertTrue(expected <= exported)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(PackageTestCase, 'test'))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
