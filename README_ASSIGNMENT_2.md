# Database systems: Exercise 2

## Group members

Thomas Gueutal, Bas Tobback, Billy Vanhove en Maksim Karnaukh

## Challenge: Implement one of three algorithms!

This time, we used python for our implementation and no longer jupyter notebooks. We decided to 
implement the extendible hashing algorithm where a limited number of buckets fits into memory.
All the code can be found in extendible_hashing.py

### Running

You may have to install some dependencies first.

```shell
pip install IPython pandas
```

To run the original database implementation (see [db.py](/db.py)), integrated with the new index
implementation (see [extendible_hashing.py](extendible_hashing.py)), simply run the test code in the database file.

```shell
python ./db.py
```

The index implementation file also contains basic tests related to the index implementation itself. Simply run that file
to run those tests.

```shell
python ./extendible_hashing.py
```

### Implementation (classes)

We decided to use four different datastructures:

* Bucket
* BucketValue
* BucketWrapper
* ExtendibleHashingIndex

First we have the Bucket class. This class stores the bucketID which is later used to retrieve buckets by id from 'disk'.
It also stores the  size of the local prefix in the bucket, which increases with every split that occurs with this bucket.
Each bucket has a max size, which represents the max number of BucketValues that may be stored in that 1 bucket. Lastly,
it stores the actual list of BucketValues.

BucketValue is our second class. An instance of BucketValue holds  a key and a value.
The key refers to the hashed key that is used to store elements in a bucket. The
value is the value that corresponds to this key. In our implementation of the index, we took  some liberties in
regards to our key representation within the index. Because performance is secondary for this assignment, hashing a key
will yield a python str that represents the binary form of the key, padded to length 32, in 'big' endian notation.
For example given `key = 8`, then assume `hash(key) == "00010000000000000000000000000000"`. This was done so that the
global prefixes that map to the buckets in the index can also be represented using strings. This was chiefly done to
circumvent many annoying conversions from bytes to int in python. The key values that our database would want to
submit to the index, are the userID integers of the user tuples in the database. The value member of the BucketValue
corresponds to the tuple address used in our database implementation, as a bytes object.

Note that at the top of [extendible_hashing.py](extendible_hashing.py) three "environment variables" are used. They
represent the default sizes of the BucketValue key and value members and the max size of every Bucket's BucketValue list.
We assumed that all BucketValues and Buckets would be similarly constrained in size, an contents so that our implementation
would be easier. *Only* the `ENV_BUCKET_MAX_SIZE` variable should ever be changed, adjusting the other two variables
breaks the index. Changing the `ENV_BUCKET_MAX_SIZE` variable will change the amount of elements every bucket can contain.

We want to store only a limited number of buckets in memory. All other buckets need to reside on disk. For example, we
want to keep 10 buckets in memory, even though 1000 buckets may exist. To solve this, we still keep a unique bucket ID
in memory for every bucket that is not in memory, but is instead stored on disk. This ID will allow us to find the related
bucket on disk.\
The BucketWrapper class was introduced to solve a single, related problem: When we flush a bucket to disk and evict it from memory,
how do we ensure that all keys/global prefixes that refer to it, know that the bucket has been evicted? Because for every
prefix, we essentially store a pointer to the bucket. So when evicting a bucket, we need to update all of the related
pointers. Instead, per prefix we will store a pointer to a BucketWrapper object. This wrapper has a single `contents`
member. When we evict the bucket, we will update this wrapper's contents to now contain the bucket ID instead of the
bucket object. This way, every prefix knows when the bucket is in or out of memory, and is able to fetch the bucket from
disk if needed.

Finally, we have the ExtendibleHashingIndex class which holds everything related to the algorithm.
It has a dictionary of global hash prefixes as keys and pointers to BucketWrappers as values. it also holds the length of
the largest (global) prefix and the max number of buckets that may be in memory at any point in time. Additionally, a list
of which buckets are in memory is required so that we may select a bucket to evict when another needs to be loaded from
disk. Also related is a mapping from bucket ID to a BucketWrapper object. This allows us to find the wrapper to use when
it is otherwise not known. To change the amount of buckets that may be stored in memory, manually change the
`ExtendibleHashingIndex.bucketsMaxInMemory` member in `ExtendibleHashingIndex.__init__` in [extendible_hashing.py](extendible_hashing.py).

### Implementation (methods)

Most of the functionality happens in the ExtendibleHashingIndex class. This is where you insert keys
with their values using the insert_keyval(k,v) method. This will request the bucket in which the insert
needs to happen. Then, the insert method will be called on this bucket, which returns a boolean. If False,
it means the bucket is full. In that case, the split method will be called 1 or more times to split the 
bucket into 2 new buckets. After each split, the keys from the old bucket will be redistributed over the
new buckets. This will keep happening until there is a bucket that has space to store the key we want to 
insert. \
\
What we mean with 'this will keep happening' is that if the max bucket size is 2 and there is a bucket
with values: 11010110... and 11010100... and the current prefix is 11, then if it gets split into buckets
110 and 111, both values of the old bucket will still be in the first bucket because both start with 110.
If you then need to insert the value 1101000... then you will need to split twice more before there is a
new bucket that has enough space to hold the new key. \
\
At the end, the insertion should be successful. At first, we tested the algorithm without being concerned
about a difference between memory and disk (we just loaded all buckets in memory to test the
algorithm itself). We randomised a list of 10000 user_ids as key and value and inserted them in the
random order that was generated by the random.shuffle() method, provided by pythons random library.
The reason we did this, is because we feel like if 10000 insertions happen in a random order, and it 
ends in a valid state every time, the algorithm is likely to work because a failure in one of the 10000
insertions will mean that the algorithm ends in an invalid state. \
\
To check this, we created 2 functions: `isValid()` and `getViolations()`. The `isValid()` function will
return True if the ExtendibleHashingIndex is in a valid state at some point in time (you can call
this function whenever you want). If it returns False, you know that something is wrong but you don't know
what exactly is wrong. That's where the `getViolations()` function comes into play. If you print the output
of this function, all the violations in the ExtendibleHashingIndex will be printed. \
Those functions are used purely for debugging reasons, but also allow us to be sure that future updates
to our code didn't contain any errors, such as when we implemented the disk storage.

### Implementation (disk storage vs memory)

Our implementation only allows to have a certain number of buckets in memory at any point in time. Because
of that, we needed to make sure to write buckets to a file. When we do a `get_bucket()` operation, we will
either get the bucket directly from memory or, if the bucket isn't present in memory, load it from disk and
replace a bucket that is in memory. Another challenge was encountered when we did the split operation. Say x buckets
may be present in memory. If all x places are filled and you then do a split, then 1 bucket needed to have 
its changes written to disk, to make place for the 2nd bucket that is created in the split. \
\
However, a challenge we faced was that the buckets to which the pointers to buckets point, could not all be 
saved in memory. We resolved this by having a wrapper called BucketWrapper, which holds a Union object with
a Bucket and integer as attribute. The integer is the keyhash (which can be saved in memory) and the Bucket
is the Bucket corresponding to the keyhash. So, the bucket pointers point to BucketWrapper objects now. If
the content of the Union is an integer, you can conclude that the bucket isn't in memory so the first operation
is to load it to memory. If the Union is a Bucket, then the bucket is already present in memory, so we don't 
need to do additional operations. \
\
The way the data is stored 'on disk' is by assuming that each bucket has a fixed size. That is, we reserve a fixed size
block of memory on-file for every bucket. The block contains space for: The local prefix size member, the maximum length
of the BucketValue list, the current length of the BucketValue list and space to store the maximum amount of BucketValues.
So if the max length of the BucketValue list is 10, but it currently contains 4 items, then the block still contains
space for all 10 potential items. With this assumption,  we can easily calculate the offset of the bucket in the binary
file, and we wouldn't have to parse the entire file each time.

We are only able to assume that every bucket requires the same fixed amount of space, 207 bytes in out implementation,
because we assume the BucketValue's key and value members always have a size of respectively 4B and 16B on disk. These
numbers were calculated as follows:

* key: our database in db.py makes use of a fixed size user ID value for every variable length tuple, namely 4B. The string representation of the keys in the index will thus contain 32 "binary characters" and take 4B in the index's storage file
* value: our database in db.py makes use of a fixed size tuple address of 16B

The use of these "fixed" values is more or less generalized in our index implementation. By editing the environment
variables at the top of the [extendible_hashing.py](extendible_hashing.py) file, the changed sizes should be taken
into account by the index. But a mismatch between the [db.py](db.py) database and the index will be created if only
the environment variables are changed.

