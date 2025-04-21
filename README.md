# Stash: stable hash and object stash

Stash assigns a stable hash to arbitrary Python objects, mutable or immutable,
based on its state at the time of hashing.

```python
>>> import stash
>>> d = [{1: 2}, 3]
>>> h = stash.hash(d)
>>> h.hex()
'acb6b358dde6ee740b18dff8232cce8f'
```

## Why not use Python's built-in [hash](https://docs.python.org/3/library/functions.html#hash)?

Python's hash differs from the stash hash in three important ways: 1. it is
supported only by a limited class of immutable objects; 2. it is not stable
between restarts, i.e. after restarting Python the same object may be assigned
a different hash, and 3. Python promises only that an equal objects have equal
hashes. Stash also guarantees the converse: equal hashes imply some notion of
object equality.

## Don't make promises you cannot keep.

You're referring to the fact that since we are mapping an infinite space of
potential objects to a finite space of hashes, there are bound to be
collisions. This is true. But by keeping track of previously seen objects we
can guarantee uniqueness within that set. If a collision happens we raise an
exception rather than return a colliding hash.

## When a collision happens.

This is the question. By using a 128 bit hash function with good distribution
properties (we use cityhash) the chance of a collision occuring is exceedingly
small. To quantify this: at 128 bits it takes an input set of 18 quintillion
(2^64) objects for the expected number of collisions to reach 1. This makes it
permissible to make collisions an unrecoverable error in most applications.

## What is the main use case?

Caching. If the output of a function is determined entirely by its arguments,
then it may be worthwhile to hold on to this value in case the function is
called with the same set of arguments later. However, this means having to make
potentially expensive deep comparisons to all previously seen arguments every
time we call the function. Worse, it also means having to make deep copies of
all the arguments to protect against future external mutations. All of this is
solved by making a hash of the arguments, and comparing it against earlier
hashes, which is precicely what stash provides.

## How does it work?

In short, stash serializes an object to bytes and hashes the serialization.

## Wait, can't we just hash a [pickle](https://docs.python.org/3/library/pickle.html) stream then?

Well, yes. But pickle stores more than what you are likely interested in, such
as the insertion order of dictionaries, so that `{'a': 1, 'b': 2}` and `{'b':
2, 'a': 1}` would end up receiving different hashes resulting in a cache miss.
Likewise, objects that contain multiple references to an object receive a
different hash than one references multiple copies. Stash loosely follows
Python's equality operator to decide which objects are assigned a unique hash.

## Loosely?

There is a fundamental problem with objects that do not test equal to
themselves, such as `float('nan')`: since the assigned hash is equal to itself,
we cannot identify object equality with hash equality. It is also not possible
to honour user defined `__eq__` methods, so we go by
[state](https://docs.python.org/3/library/pickle.html#object.__getstate__)
instead. Lastly there is an issue with `True`, `1` and `1.0` all testing equal.
This one is not fundamental, as we could very well assign all these objects the
same hash, but it adds some overhead, to no clear benefit as it is not at all
given that functions treat these objects the same. So here we make the
pragmatic choice of not doing the extra work.

## Can you say a bit more about how this works internally?

Stash works by recursively [reducing an
object](https://docs.python.org/3/library/pickle.html#object.__reduce__) and
stashing the components, which directly explains how common values are
deduplicated: stashing the same object twice simply returns a reference to an
existing hash entry. The resulting collection of hashes is bundled and hashed
to form the hash of the object, [Merkle
tree](https://en.wikipedia.org/wiki/Merkle_tree)-style. A detailed overview of
the protocol can be found [here](PROTOCOL.md).

## Reducing objects recursively sounds slow. Is it slow?

Stash is implemented in rust for minimum overhead. It also keeps track of
object ids seen before during serialization, to avoid recursing into the same
object several times over. Stash is faster than pickle without collision
checks, or roughly half as fast with in-memory collision checks.

## This all sounds great. Can I use it yet?

Better not. The project is under active development and the protocol not
finalized, so none of the stability guarantees are worth much yet. Hopefully
soon though! Watch this space for releases to stay up to date.
