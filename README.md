# Stash: stable hash and object stash

Stash assigns a stable hash to arbitrary Python objects, mutable or immutable,
based on its state at the time of hashing.

```python
>>> import stash
>>> d = [{1: 2}, 3]
>>> h = stash.hash(d)
>>> h.hex()
'84d58933596c197c74fa87f65d9a646de1481f54c9fec34ab9805bc4bd24ba46'
```

## Why not use Python's built-in [hash](https://docs.python.org/3/library/functions.html#hash)?

Python's hash differs from the stash hash in three important ways: 1. it is
supported only by a limited class of immutable objects; 2. it is not stable
between restarts, i.e. after restarting Python the same object may be assigned
a different hash, and 3. Python promises only that an equal objects have equal
hashes. Stash also promises the converse: equal hashes imply object equality.

## How is that useful?

The primary use case is caching. If the output of a function is determined
entirely by its arguments, then it may be worthwhile to hold on to this value
in case the function is called with the same set of arguments later. However,
this means we also must hold on to all the arguments, and make potentially
expensive deep comparisons every time we call the function to decide if they
were seen before. It also means that we may need make deep copies of everything
to protect against future external mutations. All of this is solved by making a
fingerprint of the arguments, and comparing it against earlier fingerprints,
which is precicely what the stash hash provides.

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
themselves, such as `float('nan')`. Since the assigned hash is equal to itself,
we cannot identify object equality with hash equality. Another issue is with
`True`, `1` and `1.0` all testing equal. This is not fundamental, as we could
very well assign all these objects the same hash, but it adds a lot of overhead
to check for every float if it matches an integer, to no clear benefit as it is
not at all given that functions treat these objects the same. So here we make
the pragmatic choice of not doing the extra work.

## And what if my function does care about things like insertion order?

In that case the standard stash hash is insufficient. Ideally, the argument
fingerprint incorporates precisely and only that what is used by the function,
but that requires tight integration with the function itself. As an external
utility, stash covers only two scenarios: the default mode that distinguishes
on the basis of equality, and a 'strict' mode which includes all potential
distinctions, such as insertion order and internal references. The latter would
equal a pickle based hash.

## Ok, and then we map argument hashes to the stored return value. Got it.

You do want to be careful that the returned value is not mutated after handing
it out, as that will mess with your cache. Best is to only hand out deep copies
of the version you hold onto. The stash serialization can help with that.

## The what now?

Remember that the stash hash is based on a serialization? It is possible to
interact with this serialization directly via `dumps` and `loads`. These
functions are functionally equivalent to their pickle counterparts, except that
the returned and ingested bytes are hashes, that act as pointers into a
database.

## Why not just use [deepcopy](https://docs.python.org/3/library/copy.html#copy.deepcopy)?

That would work well enough, but stash has one big advantage that could drive
memory down: when different return have certain nested components in common,
then these components are stored only once in the stash database. What's more,
if the database is directed to persistent storage, then this even works between
Python restarts. Think of the stash as a single object database that you can
keep adding items to in return for a hash.

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
object several times over. Overall the speed of stash is roughly on par with
that of pickle when combined with an in-memory database.

## This all sounds great. Can I use it yet?

Better not. The project is under active development and the protocol not
finalized, so none of the stability guarantees are worth much yet. Hopefully
soon though! Watch this space for releases to stay up to date.
