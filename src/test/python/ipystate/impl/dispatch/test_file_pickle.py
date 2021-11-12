import copyreg
import pickle
import unittest

from ipystate.impl.dispatch.common import CommonDispatcher


class SerializationFileTest(unittest.TestCase):

    def test_r_file(self):
        with open('test_r.txt', 'w') as f2:
            f2.write('aba')
        f = open('test_r.txt', 'r')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        f.close()
        f1.close()
        self.assertEqual(str(f), str(f1))

    def test_r_equal_read(self):
        with open('test_r_equal', 'w') as f2:
            f2.write('aba')
        f = open('test_r_equal', 'r')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f1.read()), 'aba')
        self.assertEqual(str(f1.read()), '')
        f.close()
        f1.close()

    def test_rb_file(self):
        with open('test_open_rb.txt', 'w') as f2:
            f2.write('aba')
        f = open('test_open_rb.txt', 'rb')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        self.assertEqual(str(f1.read()), str(b'aba'))
        self.assertEqual(str(f1.read()), str(b''))
        f.close()
        f1.close()
        self.assertEqual(str(f), str(f1))

    def test_rplus_file(self):
        with open('test_open_r+.txt', 'w') as f2:
            f2.write('aba')
        f = open('test_open_r+.txt', 'r+')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        self.assertEqual(str(f1.read()), 'aba')
        try:
            f1.write('abacaba')
        except:
            self.fail('Expected not raises')
        f.close()
        f1.close()

    def test_rbplus_file(self):
        with open('test_open_rb+.txt', 'w') as f2:
            f2.write('aba')
        f = open('test_open_rb+.txt', 'r+')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        self.assertEqual(str(f1.read()), 'aba')
        try:
            f1.write('abacaba')
        except:
            self.fail('Expected not raises')
        f.close()
        f1.close()

    def test_w_file(self):
        f = open('test_open_w.txt', 'w')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        f.close()
        f1.close()
        self.assertEqual(str(f), str(f1))

    def test_wb_file(self):
        f = open('test_open_wb.txt', 'wb')
        copyreg.pickle(type(f), CommonDispatcher._reduce_filehandle)
        f1_ = pickle.dumps(f)
        f1 = pickle.loads(f1_)
        self.assertEqual(str(f.__enter__()), str(f1.__enter__()))
        f.close()
        f1.close()
        self.assertEqual(str(f), str(f1))

    def test_stdin(self):
        import sys
        stdin = sys.__stdin__
        copyreg.pickle(type(stdin), CommonDispatcher._reduce_filehandle)
        stdin1_ = pickle.dumps(stdin)
        stdin1 = pickle.loads(stdin1_)
        self.assertEqual(str(stdin), str(stdin1))
