# Stash protocol

This is a work in progress document.

Every native Python type (int, float, list, tuple, etc) is assigned a single
byte token. Any serialized object consists of a token followed by an arbitrary
length byte sequence that represents the value in a type specific manner:

    [token] [bytes ...]

If the serialisation references child objects, for example in the case of a
list, then this reference typically takes the form of a byte sequence of at
maximum 256 bytes that we refer to as a chunk. For example, a list of three
items is serialized as:

    [list-token] [chunk1 ...] [chunk2 ...] [chunk3 ...]

A chunk starts with a single byte that encodes the number of subsequent bytes
to deserialize for this object. If the length of the serialization exceeds 255
then the object is stored separately in a content-addressable manner, and the
length byte is set to zero to indicate that what follows is a (fixed length)
hash:

    Inline chunk: [length] [token] [bytes ...]
    Hashed chunk: [0] [hash -> token bytes]

Note that, by this mechanism, only objects that are serialized to more than 255
bytes are stored as separate entries in the database.

In order to obtain a hash that is valid for equality testing, as well as
maximize deduplication in storage, items of sets and dictionaries are sorted by
their serialization prior to hashing. A consequence of this is that dictionary
loses its insertion order. Other information that is necessarily lost is the
distinction between "==" an "is" identities, as any hash that retains this
information cannot simultaneously test equal for both situations.

To retain these key properties, and be a drop-in solution for pickle, stash
distinguishes between a comparison hash and a deserialization hash. The former
is simply the hash of the object's serialized form, which does not typically
correspond to a database entry. The latter is the hash of a traversal object,
which does correspond to a database entry, and which is structured as:

    [traversal-token] [chunk ...] [traversal info ...]

The traversal info contains information that was written sequentially during
derialization, and can likewise be processed in sequence during
deserialization. For any chunk it indicates whether the object is to be newly
formed, or referenced to an earlier formed object. Additionally, if the newly
formed object is a dictionary, then the traversal info contains the insertion
order, referencing chunks by their ordered index.
