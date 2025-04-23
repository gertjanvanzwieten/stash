import stash, math, unittest, tempfile

try:
    import numpy
except ImportError:
    numpy = None


class MyClass:
    def __init__(self, x):
        self.x = x
    def __eq__(self, other):
        return isinstance(other, MyClass) and self.x == other.x


class MyReduceableClass(MyClass):
    def __reduce__(self):
        return MyReduceableClass, (self.x,)


class Base(unittest.TestCase):

    def check(self, obj, eq=lambda x: x):
        h = self.db.hash(obj)
        obj_ = self.db.unhash(h)
        self.assertIs(type(obj), type(obj_))
        self.assertEqual(eq(obj_), eq(obj))
        return h

    def test_int(self):
        self.check(-1)
        self.check(0)
        self.check(1)
        self.check(127)
        self.check(128)
        self.check(-128)
        self.check(-129)

    def test_float(self):
        self.check(0.)
        self.check(1e15)
        self.check(1e-15)
        self.check(-1e15)
        self.check(-1e-15)
        self.check(float('inf'))
        self.check(float('-inf'))
        self.check(float('nan'), eq=math.isnan)

    def test_list(self):
        self.check([1, 2, 3])
        self.check([1, [2, [3]]])

    def test_tuple(self):
        self.check((1, 2, 3))
        self.check((1, (2, (3,))))

    def test_set(self):
        self.check({1, 2, 3})

    def test_frozenset(self):
        self.check(frozenset({1, 2, 3}))

    def test_bytes(self):
        self.check(b'abc')

    def test_bytearray(self):
        self.check(bytearray(b'abc'))

    def test_str(self):
        self.check('abc')

    def test_dict(self):
        d1 = {'a': 1, 'b': 2, 'c': 3}
        h1 = self.check(d1)
        d2 = {'b': 2, 'c': 3, 'a': 1}
        h2 = self.check(d2)
        self.assertEqual(h1, h2)

    def test_none(self):
        self.check(None)

    def test_true(self):
        self.check(True)

    def test_false(self):
        self.check(False)
        self.check(None)

    def test_reduce(self):
        self.check(MyClass(10))
        self.check(MyReduceableClass(10))

    def test_global(self):
        self.check(MyClass)
        self.check(MyReduceableClass)

    @unittest.skipIf(numpy is None, "numpy is not installed")
    def test_numpy_array(self):
        self.check(numpy.arange(.5, 12).reshape(3, 4), eq=numpy.ndarray.tolist)

    @unittest.skipIf(numpy is None, "numpy is not installed")
    def test_dispatch_table(self):
        self.check(numpy.sin)


class Nil(Base):

    def check(self, obj, eq=lambda x: x):
        return stash.hash(obj)


class PyDB(Base):

    def setUp(self):
        self.d = {}
        self.db = stash.PyDB(self.d)

    def assertLength(self, obj, n):
        h = self.db.hash(obj)
        self.assertEqual(len(self.d[h]), n)

    def test_int(self):
        self.assertLength(-1, 2)
        self.assertLength(0, 1)
        self.assertLength(1, 2)
        self.assertLength(127, 2)
        self.assertLength(128, 3)
        self.assertLength(-128, 2)
        self.assertLength(-129, 3)


class RAM(Base):

    def setUp(self):
        self.db = stash.RAM()


class FsDB(Base):

    def setUp(self):
        c = tempfile.TemporaryDirectory()
        self.addCleanup(c.__exit__, None, None, None)
        self.db = stash.FsDB(c.__enter__())


class FileDB(Base):

    def setUp(self):
        c = tempfile.NamedTemporaryFile()
        self.addCleanup(c.__exit__, None, None, None)
        self.dbpath = c.__enter__().name
        self.db = stash.FileDB(self.dbpath)

    def test_reload(self):
        obj1 = 1, 2, 3
        obj2 = obj1, "abc"
        h1 = self.db.hash(obj1)
        h2 = self.db.hash(obj2)
        del self.db
        db = stash.FileDB(self.dbpath)
        self.assertEqual(db.hash(obj1), h1)
        self.assertEqual(db.hash(obj2), h2)


del Base
