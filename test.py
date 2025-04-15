import stash, math, unittest

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


class Stash(unittest.TestCase):

    def check(self, obj, eq=lambda x: x):
        d = {}
        db = stash.PyDB(d)
        b = db.dumps(obj)
        obj_ = db.loads(b)
        self.assertIs(type(obj), type(obj_))
        self.assertEqual(eq(obj_), eq(obj))
        s = d[b]
        self.assertEqual(s[0], 0)
        return s[1]

    def test_int(self):
        self.assertEqual(self.check(-1), 2)
        self.assertEqual(self.check(0), 1)
        self.assertEqual(self.check(1), 2)
        self.assertEqual(self.check(127), 2)
        self.assertEqual(self.check(128), 3)
        self.assertEqual(self.check(-128), 2)
        self.assertEqual(self.check(-129), 3)

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
        self.check({'a': 1, 'b': 2, 'c': 3})
        self.check({1: 'a', 2: 'b', 3: 'c'})

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

    def test_dedup(self):
        db = stash.PyDB({})
        bigobj = b'abc' * 999
        b = db.dumps([bigobj, bigobj])
        bigobj1, bigobj2 = db.loads(b)
        self.assertIs(bigobj1, bigobj2)
        self.assertIsNot(bigobj1, bigobj)
        self.assertEqual(bigobj1, bigobj)
