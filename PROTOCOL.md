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
