# Stash: stable hash and object stash

Stash is a pickle alternative that stores objects in a central database.

Like the equivalent pickle functions, `stash.dumps` returns a byte sequence
that can be loaded back via `stash.loads`; only the stash sequence is a fixed
length handle into the database. As handles are stable, the same mechanism can
be used to generate hashes for arbitrary Python objects, optionally combined
with a null database if deserialization is not required.

The key advantage of central storage is deduplication: objects that are stashed
twice are stored once. This applies at any level of nesting, as contained
objects are stashed recursively. This makes stash particularly well suited for
caching purposes, where individual serializations might result in excessive
duplication.

The hash itself can be useful in memoization strategies. Unlike Python's
built-in hash function, stash hashes are stable between restarts and not
limited to immutable objects, which allows them to be used for the persistent
storage of a much wider class of function arguments.

## Example

```python
>>> import stash
>>> db = stash.FsDB('/path/to/db/')
>>> obj = ['foo', {'bar': 'baz'}]
>>> h = db.dumps(obj)
>>> db.loads(h) == obj
True
```

## Differences to pickle

Stash differs from pickle in a number of important ways.

- Dictionary insertion order is not preserved.

  Stash upholds the rule that the hash of two objects must be equal if the
  objects test equal. Since dictionary equality in Python disregards the
  insertion order, this implies that stash cannot preserve it.

- Object identities are not preserved

  When a Python object contains multiple references to a second object, pickle
  preserves that these references are "is" rather than "==" identical and
  unpickles accordingly. Stash deserializes all objects as individual copies,
  with the exception of immutable types such as integers and strings.
