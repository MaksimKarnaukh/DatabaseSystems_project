from typing import List, Tuple, Dict, Union, Callable

#
# ENVIRONMENT VARIABLES
#

# The default size of the BucketValue keys as bytes
ENV_BUCKET_VALUE_KEY_SIZE: int = 4
# The default size of the BucketValue values as bytes
ENV_BUCKET_VALUE_VALUE_SIZE: int = 16
# The default size of the Bucket list
ENV_BUCKET_MAX_SIZE: int = 10


#
# CODE
#

class BucketValue(object):
    def __init__(self, key: str, value: bytes):
        """BucketValue constructor.

        :param key: A string key that is required to be of a fixed size (see BucketValue.get_env_bucketvalue_key_size())
        :param value: A bytes value that is required to be of a fixed size (see BucketValue.get_env_bucketvalue_value_size())
        """
        assert isinstance(key, str), f"A {BucketValue.__name__} must have a {str.__name__} type key"
        assert isinstance(value, bytes), f"A {BucketValue.__name__} must have a {bytes.__name__} type value"
        assert len(key) == BucketValue.get_env_bucketvalue_key_size() * 8, f"Invalid {BucketValue.__name__} key length: got {len(key)}, expected {BucketValue.get_env_bucketvalue_key_size()}"
        assert len(value) == BucketValue.get_env_bucketvalue_value_size(), f"Invalid {BucketValue.__name__} value length: got {len(value)}, expected {BucketValue.get_env_bucketvalue_value_size()}"

        self.key: str = key
        self.value: bytes = value

    def __str__(self):
        return str(self.key) + " : " + str(self.value)

    def __repr__(self):
        return self.__str__()

    def __bytes__(self):
        """
        Convert BucketValue to a byte string of length 20 bytes.

        :return: bytestring of length 20
        """
        # make a bytearray of key and value
        key_integer = eval('0b' + self.key)
        key_bytes = key_integer.to_bytes(4, byteorder='big')
        value_bytes = bytes(self.value)
        return key_bytes + value_bytes

    @classmethod
    def from_bytes(cls, byte_data):
        """
        Reconstructs a BucketValue object from its byte representation.

        :param byte_data: Byte representation of the BucketValue.
        :return: Reconstructed BucketValue object.
        """
        key_bytes = byte_data[:4]
        value_bytes = byte_data[4:]

        # Convert key bytes to an integer and then to a binary string
        key_integer = int.from_bytes(key_bytes, byteorder='big')
        key_binary_string = bin(key_integer)[2:].zfill(32)

        # Create and return the BucketValue object
        return cls(key_binary_string, value_bytes)

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value

    @staticmethod
    def get_env_bucketvalue_key_size() -> int:
        """Get the environment's default BucketValue key size; the amount of bytes
        needed to store the key on disc.
        """
        return ENV_BUCKET_VALUE_KEY_SIZE

    @staticmethod
    def get_env_bucketvalue_value_size() -> int:
        """Get the environment's default BucketValue value size; the amount of bytes
        needed to store the value on disc.
        """
        return ENV_BUCKET_VALUE_VALUE_SIZE

    @staticmethod
    def get_env_bucketvalue_size() -> int:
        """Get the environment's default BucketValue total size; the amount of bytes
        needed to store the BucketValue on disc.
        """
        return BucketValue.get_env_bucketvalue_key_size() + BucketValue.get_env_bucketvalue_value_size()


class Bucket(object):
    def __init__(self, bucket_id: int, local_prefix_size: int = 1, max_size: int = 10, bucket_values: List[BucketValue] = None):
        """Bucket constructor.

        :param bucket_id: The (unique) ID of the bucket
        :param local_prefix_size: The local prefix length/size of the bucket
        :param max_size: The maximum amount of elements allowed in the BucketValue list
        :param bucket_values: (optional) a list of initial BucketValues
        """
        self.bucketID = bucket_id
        self.localPrefixSize: int = local_prefix_size

        self.maxSize: int = max_size
        self.list: List[BucketValue] = [] if bucket_values is None else bucket_values

    def __str__(self):
        result = f"<ID {self.bucketID}, local {self.localPrefixSize}, maxSize {self.maxSize}, curSize {len(self.list)}> [\n"
        for elem in self.list:
            result += "    " + str(elem) + '\n'
        result += ']'
        return result

    def __repr__(self):
        return self.__str__()

    def __len__(self):
        return len(self.list)

    def __eq__(self, other):
        return self.list == other.list

    def __bytes__(self):
        # make a bytearray of localPrefixSize, maxSize, currentSize, bucketID, and then the bucketValues.
        # in total this is: 1+1+1+4+10*20 = 207 bytes for one bucket
        local_prefix_size_bytes = self.localPrefixSize.to_bytes(1, byteorder='big')  # never a value > 32
        max_size_bytes = self.maxSize.to_bytes(1, byteorder='big')  # never a value > 255
        cur_size_bytes = len(self.list).to_bytes(1, byteorder='big')  # never a value > 255
        bucket_id_bytes = self.bucketID.to_bytes(4, byteorder='big')  # max 2^32 buckets
        bucket_values_bytes: bytes = bytes()
        for bucket_value in self.list:
            bucket_values_bytes += bytes(bucket_value)
        return local_prefix_size_bytes + max_size_bytes + cur_size_bytes + bucket_id_bytes + bucket_values_bytes

    @staticmethod
    def get_env_bucket_max_size() -> int:
        """Get the environment's default Bucket list size; the amount of elements
        a Bucket should at most contain.
        """
        return ENV_BUCKET_MAX_SIZE

    @staticmethod
    def get_env_bucket_bytes_max_size() -> int:
        """Determine the maximal size of the Bucket when
        it is converted to bytes.

        :return: The max bytes size
        """
        # The calculation incorporates the nr of bytes needed
        # to store each of the following Bucket properties:
        #   local_prefix_size +
        #   max_list_size +
        #   current_list_size +
        #   bucket ID +
        #   env_max_list_size * env_bucketvalue_size
        env_max_list_size = Bucket.get_env_bucket_max_size()
        env_bucketvalue_size = BucketValue.get_env_bucketvalue_size()
        return 1 + 1 + 1 + 4 + env_max_list_size * env_bucketvalue_size

    @classmethod
    def from_bytes(cls, byte_data: bytes, key_len: int, value_len: int):
        """Create a Bucket object from bytes.

        :param cls: (Implicit) Bucket class
        :param bytes_data: The bytes to parse
        :param key_len: The amount of bytes used to encode the BucketValue keys
        :param value_len: The amount of bytes used to encode the BucketValue values
        :return: The created bucket object
        """
        list_start_byte: int = 1 + 1 + 1 + 4        # The start byte nr of the bucket value list
        bucketvalue_len: int = key_len + value_len

        local_prefix_size = int.from_bytes(byte_data[0:1], byteorder='big')
        max_size = int.from_bytes(byte_data[1:2], byteorder='big')
        cur_size = int.from_bytes(byte_data[2:3], byteorder='big')
        bucket_id = int.from_bytes(byte_data[3:list_start_byte], byteorder='big')


        bucket_values = []
        for i in range(list_start_byte, list_start_byte + cur_size * bucketvalue_len, bucketvalue_len):
            key_bytes: bytes = byte_data[i:i + 4]
            value_bytes: bytes = byte_data[i + 4:i + bucketvalue_len]

            key = bin(int.from_bytes(key_bytes, byteorder='big'))[2:].zfill(32)

            bucket_values.append(BucketValue(key, value_bytes))

        # Create and return the Bucket object
        return cls(bucket_id, local_prefix_size, max_size, bucket_values)

    def get_local_prefix_size(self) -> int:
        return self.localPrefixSize

    def set_local_prefix_size(self, size: int):
        self.localPrefixSize = size

    def get_max_size(self) -> int:
        return self.maxSize

    def get_bucket_values(self) -> List[BucketValue]:
        return self.list

    def insert(self, value: BucketValue) -> bool:
        """
        Inserts a value of type BucketValue into the bucket.
        Replaces the BucketValue's value if the key already exists
        in this Bucket.

        :param value: value to insert
        :return: True if the value was inserted, False otherwise
        """
        found = self.search(value.key)
        if found is not None:
            found.value = value.value
            return True

        if len(self.list) < self.maxSize:
            self.list.append(value)
            return True
        return False

    def delete(self, key) -> bool:
        """
        Deletes the first item with the given key from the bucket.
        :param key: hashed key
        :return: True if the item was found and deleted, False otherwise
        """
        for item in self.list:
            if item.get_key() == key:
                self.list.remove(item)
                return True
        return False

    def search(self, key) -> Union[BucketValue, None]:
        """
        Searches for the first item with the given key in the bucket.
        :param key: hashed key
        :return: item of type 'BucketValue' in the bucket with the given key, None if not found
        """
        for item in self.list:
            if item.get_key() == key:
                return item
        return None


def hash_function_str(key: int) -> str:
    """Hashes a key and returns the hash value.
    In this case, the key is converted to a binary string and padded to 32 bits.

    :return: The hash as a string
    """
    binary_string = bin(key)[2:]
    # Ensure the binary string has 32 bits by left-padding with zeros if needed
    padded_binary_string = binary_string.zfill(32)
    result = padded_binary_string[::-1]
    return result


def get_hash_prefix(keyHash: str, prefixSize: int) -> str:
    """Determine the key hash prefix.
    
    :param keyHash: The hash value to extract the prefix from
    :return: The prefix of the hash value.
    """
    return keyHash[: prefixSize]


class BucketWrapper:
    """A class that wraps a Bucket, so that a Bucket object cab
    be evicted from memory or loaded into memory in one operation.

    Setting the contents of the wrapper makes it so that every
    prefix that is related to a specific Bucket, now refers to
    the same passed Bucket object or bucket ID.
    """
    def __init__(self, initial_content: Union[Bucket, int]):
        self.contents: Union[Bucket, int] = initial_content


class ExtendibleHashingIndex(object):
    def __init__(self):

        self.globalHashPrefixSize: int = 1

        self.bucketsFixedSize: int = Bucket.get_env_bucket_bytes_max_size()
        self.bucketsMaxInMemory: int = 6
        self.bucketsDataFileName: str = "buckets_data.dat"
        self.bucketsIDCounter: int = 0

        bucket0: Bucket = Bucket(self.reserve_bucket_ID())
        bucket1: Bucket = Bucket(self.reserve_bucket_ID())
        bucketWrapper0: BucketWrapper = BucketWrapper(bucket0)
        bucketWrapper1: BucketWrapper = BucketWrapper(bucket1)
        self.bucketPointers: Dict[str: Union[int, BucketWrapper]] = {
            "0": bucketWrapper0,
            "1": bucketWrapper1
        }
        # List of bucket IDs
        self.bucketsInMemory: List[Bucket] = [bucket0, bucket1]
        # mapping from bucket ID to BucketWrapper
        self.bucketsToWrapper: Dict[int, BucketWrapper] = {
            bucket0.bucketID: bucketWrapper0,
            bucket1.bucketID: bucketWrapper1,
        }

    def __str__(self):
        reversedDict = dict()
        bucketByID = dict()
        for k, v in self.bucketPointers.items():
            v = v.contents if isinstance(v.contents, Bucket) else self.read_bucket(v.contents)
            bucketByID[v.bucketID] = v
            if v.bucketID in reversedDict.keys():
                reversedDict[v.bucketID].append(k)
            else:
                reversedDict[v.bucketID] = [k]

        printStr = ""
        for k, v in reversedDict.items():
            printStr += 'prefix: ' + ','.join(v) + '\n'
            printStr += bucketByID[k].__str__() + '\n'

        return printStr

    def reserve_bucket_ID(self) -> int:
        """Reserve a bucket ID value.
        Each subsequent call of this method increments
        the bucket ID counter.

        :return: A unique bucket ID
        """
        oldValue: int = self.bucketsIDCounter
        self.bucketsIDCounter += 1
        return oldValue

    def get_bucket(self, prefix: str) -> Tuple[Union[Bucket, None], Union[BucketWrapper, None]]:
        """Retrieve the bucket corresponding to the given prefix.

        :param prefix: A prefix of the full key hash to find the bucket for
        :return: (
            The corresponding Bucket if it exists, else None,
            The corresponding BucketWrapper if it exists, else None,
        )
        """
        bucket_wrapper: BucketWrapper = self.bucketPointers.get(prefix, None)
        assert bucket_wrapper is not None, f"Invalid prefix was used to get a bucket, no {BucketWrapper.__class__.__name__} was found for the prefix '{prefix}'"
        bucket: Union[int, Bucket] = bucket_wrapper.contents
        if isinstance(bucket, int):
            bucket = self.read_bucket(bucket)
            bucket_wrapper.contents = bucket
            self.set_bucket(prefix, bucket_wrapper)
            print(f"load {bucket.bucketID}", flush=True)  # TODO delete delete delete delete delete
            assert len(self.bucketsInMemory) <= self.bucketsMaxInMemory, f"Too many buckets in memory: {len(self.bucketsInMemory)} > {self.bucketsMaxInMemory}"
        return bucket, bucket_wrapper

    def set_bucket(self, prefix: str, bucketWrapper: BucketWrapper) -> None:
        """Store the given bucket or bucket ID under the given prefix.
        Evicts another bucket from memory if there is no more room in-memory.

        :param prefix: The prefix of the full key hash to set the bucket or bucket ID for
        :param bucketWrapper: The bucket wrapper to map the prefix to
        """
        wrapperContents: Union[Bucket, int] = bucketWrapper.contents
        bucketID: int = wrapperContents if isinstance(wrapperContents, int) else wrapperContents.bucketID

        # Always overwrite wrapper, is actually redundant
        # if BucketWrapper is not new

        if isinstance(wrapperContents, Bucket):
            self.bucketsToWrapper[bucketID] = bucketWrapper
            self.write_bucket(wrapperContents)
            
            bucketNotInMem: bool = wrapperContents not in self.bucketsInMemory
            if len(self.bucketsInMemory) >= self.bucketsMaxInMemory and bucketNotInMem:
                evicted_bucket: Bucket = self.bucketsInMemory.pop(0)
                evicted_bucket_wrapper: BucketWrapper = self.bucketsToWrapper[evicted_bucket.bucketID]
                self.write_bucket(evicted_bucket)   # flush bucket before in-mem eviction
                evicted_bucket_wrapper.contents = evicted_bucket.bucketID   # Do in-mem eviction
                print(f"evict {evicted_bucket.bucketID}   ({[b.bucketID for b in self.bucketsInMemory]})", flush=False)  # TODO delete delete delete delete delete
    
            if bucketNotInMem:
                print([b.bucketID for b in self.bucketsInMemory])
                self.bucketsInMemory.append(wrapperContents)
            
        else:
            self.bucketsToWrapper[bucketID] = bucketWrapper

        self.bucketPointers[prefix] = bucketWrapper

    def get_hash_from_key(self, key: int, hash_function: Callable=hash_function_str):
        """Transform the given key into a hash.

        :param key: The key to hash
        :param 
        :return: The key hash
        """
        return hash_function(key)
    
    def get_prefix_from_key_hash(self, keyHash: str) -> str:
        """Extract the index's global prefix from the given key hash.

        :param keyHash: The key hash to extract the prefix from
        :return: The global prefix
        """
        return get_hash_prefix(keyHash=keyHash, prefixSize=self.globalHashPrefixSize)

    def get(self, key):
        """
        Returns the first item with the given key from the index.

        :param key: non-hashed key
        :return:
        """
        # first, get the bucket associated with the key
        keyHash: str = self.get_hash_from_key(key=key)
        prefix: str = self.get_prefix_from_key_hash(keyHash=keyHash)
        bucket, _ = self.get_bucket(prefix=prefix)
        # then, get the item from the bucket
        return bucket.search(keyHash)

    def insert_keyval(self, key: int, value: bytes):
        """Inserts a key-value pair into the index."""
        keyHash: str = self.get_hash_from_key(key=key)
        prefix: str = self.get_prefix_from_key_hash(keyHash=keyHash)
        bucket, bucketWrapper = self.get_bucket(prefix=prefix)
        bucketValue: BucketValue = BucketValue(keyHash, value)
        success: bool = bucket.insert(bucketValue)

        # Bucket is full, split it
        if not success:
            self.split(bucketWrapper)

            # insert recursively (for in the case that the destination bucket is still full)
            self.insert_keyval(key, value)

    def delete(self, key):
        """
        Deletes the first item with the given key from the index.
        :param key: non-hashed key
        :return:
        """
        # first, get the bucket associated with the key
        keyHash = self.get_hash_from_key(key=key)
        prefix: str = self.get_prefix_from_key_hash(keyHash=keyHash)
        bucket, _ = self.get_bucket(prefix=prefix)
        # then, delete the item from the bucket
        return bucket.delete(keyHash)

    def split(self, bucketWrapper: BucketWrapper) -> None:
        """Perfom a split on the index for a given bucket.
        Perform any necessary actions after the split to
        bring the index into a valid state again.

        IMPORTANT: This method invalidates the passed *bucket* reference

        :param bucketWrapper: The wrapper of the bucket to split
        """
        # TODO: \/ Buckets are stored in pages in memory???? \/
        bucket: Bucket = bucketWrapper.contents
        assert isinstance(bucket, Bucket), f"Can only split a bucket, not a '{bucket.__class__.__name__}' type"
        assert bucket in self.bucketsInMemory, "Can only split a bucket that is in memory"
        shouldIncreaseGlobal: bool = bucket.localPrefixSize == self.globalHashPrefixSize

        # if the global prefix length is smaller than the prefix length after a split, then we need to
        # update the global prefix length and increase its length with 1, for each entry.
        if shouldIncreaseGlobal:
            newBucketPointers: Dict[str: BucketWrapper] = dict()
            for oldPrefix, oldBucketWrapper in self.bucketPointers.items():
                newPrefix0, newPrefix1 = self.get_extended_prefixes(oldPrefix)
                newBucketPointers[newPrefix0] = oldBucketWrapper
                newBucketPointers[newPrefix1] = oldBucketWrapper
            self.bucketPointers = newBucketPointers

        res0, res1 = self.split_bucket(bucket)
        newBucket0, newBucketPrefix0 = res0
        newBucket1, newBucketPrefix1 = res1

        bucketWrapper.contents = newBucket0
        self.bucketsInMemory[self.bucketsInMemory.index(bucket)] = newBucket0
        bucketWrapper0: BucketWrapper = bucketWrapper
        bucketWrapper1: BucketWrapper = BucketWrapper(newBucket1)


        # because we did a split, we need to update all related pointers
        for ptr in self.bucketPointers.keys():
            if ptr.startswith(newBucketPrefix0):
                self.set_bucket(ptr, bucketWrapper0)
            elif ptr.startswith(newBucketPrefix1):
                self.set_bucket(ptr, bucketWrapper1)

        self.globalHashPrefixSize += shouldIncreaseGlobal

        # Immediately write buckets to maintain
        # sequential bucket file structure
        self.write_bucket(newBucket0)
        self.write_bucket(newBucket1)

    def split_bucket(self, bucket: Bucket) -> Tuple[Tuple[Bucket, str], Tuple[Bucket, str]]:
        """Create two new buckets based on an "old" bucket.
        The two new buckets contain the bucket values of
        the old bucket. The old bucket is untouched.

        :return: (
            [ newBucket0, newPrefix0 ],
            [ newBucket1, newPrefix1 ],
        )
        """

        bucketValues = bucket.get_bucket_values()
        randomBucketHash = bucketValues[0].get_key()
        bucketLocalPrefix = get_hash_prefix(randomBucketHash, bucket.localPrefixSize)

        newPrefix0, newPrefix1 = self.get_extended_prefixes(bucketLocalPrefix)

        new_bucket0 = Bucket(bucket.bucketID, local_prefix_size=bucket.localPrefixSize + 1)
        new_bucket1 = Bucket(self.reserve_bucket_ID(), local_prefix_size=bucket.localPrefixSize + 1)

        for _, bucketValueObj in enumerate(bucketValues):
            bucketKey = bucketValueObj.get_key()
            keyHash = bucketKey[: len(newPrefix0)]

            if newPrefix0 == keyHash:
                new_bucket0.insert(bucketValueObj)
            elif newPrefix1 == keyHash:
                new_bucket1.insert(bucketValueObj)
            else:
                print("YOU SHOULDN'T EVER SEE THIS MESSAGE")

        return (new_bucket0, newPrefix0), (new_bucket1, newPrefix1)

    def get_extended_prefixes(self, prefix: str) -> Tuple[str, str]:
        """Convert the prefix into two extended prefixes of one 'bit' longer.

        :return: (
            prefix | '0',
            prefix | '1'
        )
        """
        return prefix + '0', prefix + '1'

    def read_bucket(self, bucketID: int) -> Bucket:
        """Read a bucket from the bucket storage file.

        :param bucketID: The bucket ID of the bucket to read
        :return: A Bucket object constructed from the read bytes
        """
        try:
            with open(self.bucketsDataFileName, "rb") as f:
                f.seek(self.bucketsFixedSize * bucketID)
                bucketBytes: bytes = f.read(self.bucketsFixedSize)
                
                return Bucket.from_bytes(
                    bucketBytes,
                    BucketValue.get_env_bucketvalue_key_size(),
                    BucketValue.get_env_bucketvalue_value_size()
                )

        except FileNotFoundError as e:
            errorMsg = f"No such bucket storage file exists. At least one bucket write MUST happen before the first bucket read."
            errorTxt = str(e) + "\n" + errorMsg
            raise FileNotFoundError(errorTxt) from e

    def write_bucket(self, bucket: Bucket) -> None:
        """Write the bytes of the bucket to the bucket storage file.

        :param bucket: bucket object to write
        """
        # Ensure file exists
        open(self.bucketsDataFileName, 'a').close()

        with open(self.bucketsDataFileName, "rb+") as file:
            bucket_bytes = bytes(bucket)

            if len(bucket_bytes) > self.bucketsFixedSize:
                raise ValueError("Bucket data size exceeds the specified record size.")

            # If the bucket data is smaller than the record size, pad it with zeros
            padded_bucket_bytes = bucket_bytes + bytes([0] * (self.bucketsFixedSize - len(bucket_bytes)))

            assert len(padded_bucket_bytes) == self.bucketsFixedSize, f"To write bucket bytes of incorrect size: got {len(padded_bucket_bytes)}, expected {self.bucketsFixedSize}"

            file.seek(self.bucketsFixedSize * bucket.bucketID)
            file.write(padded_bucket_bytes)

    def getViolations(self, exitOnViolation: bool=True) -> List[str]:
        """Collect all violations of the ExtendibleHashingIndex against
        its structure by definition.

        Violations are, for example, a key-value pair being mapped to a bucket that
        does not match its hashed-key-prefix.

        :return: A list of all violations
        """
        violations = []
        for prefix, bucketWrapper in self.bucketPointers.items():
            # A prefix is of the correct size
            isLenCorrect = len(prefix) == self.globalHashPrefixSize
            if not isLenCorrect:
                violations.append(f"bad prefix length: found {len(prefix)}, expected {self.globalHashPrefixSize}")
                if exitOnViolation:
                    return violations

            element: BucketValue
            bucket: Bucket = bucketWrapper.contents if isinstance(bucketWrapper.contents, Bucket) else self.read_bucket(bucketWrapper.contents)
            for element in bucket.list:
                if not element.key.startswith(prefix[0:bucket.localPrefixSize]):
                    violations.append(f"incorrect prefix: {prefix[0:bucket.localPrefixSize]} for bucket element: {element.key} with bucket localPrefixSize = {bucket.localPrefixSize}")
                    if exitOnViolation:
                        return violations

        return violations

    def isValid(self) -> bool:
        """Check whether the ExtendibleHashingIndex is valid.

        :return: Validity boolean
        """
        return len(self.getViolations(False)) == 0


if __name__ == "__main__":
    eh = ExtendibleHashingIndex()

    if False:
        key, value = 0, bytes([12] * BucketValue.get_env_bucketvalue_value_size())
        hashed_key = eh.get_hash_from_key(key=key)
        eh.insert_keyval(hashed_key, value=value)

        b0_prefix: str = "0"
        b0: Bucket = eh.bucketPointers[b0_prefix]

        eh.insert_keyval(hash_function_str(2), value=bytes([22] * BucketValue.get_env_bucketvalue_value_size()))
        eh.insert_keyval(hash_function_str(4), value=bytes([44] * BucketValue.get_env_bucketvalue_value_size()))

        print(b0)
        print([int.from_bytes(v, 'big') for v in b0.get_bucket_values()])
        print([v for v in b0.list[0].get_value()])

        eh.write_bucket(b0)
        eh.bucketPointers[b0_prefix] = b0.bucketID
        eh.read_bucket(b0.bucketID)
        b, _ = eh.get_bucket(b0_prefix)
        print(b)
        print(b.list[0].get_value())
        print([v for v in b.list[0].get_value()])
        print(len(b.list[0].get_value()))

    if True:
        import random
        data_set = [i for i in range(10000)] # list(range(30))
        random.shuffle(data_set)

        # insert 10000 entries in a random order
        for user_id in data_set:
            value = user_id.to_bytes(BucketValue.get_env_bucketvalue_value_size(), 'big')
            eh.insert_keyval(key=user_id, value=value)

        # TESTING
        print(eh)

        print(eh.isValid())
        print(eh.getViolations(False))
        print(len(eh.bucketPointers.keys()), "  (unique #prefixes)")
        print(len(set([(b.contents if isinstance(b.contents, Bucket) else eh.read_bucket(b.contents)).bucketID for b in eh.bucketPointers.values()])), "  (unique #buckets)")
        print(max([(b.contents if isinstance(b.contents, Bucket) else eh.read_bucket(b.contents)).bucketID for b in eh.bucketPointers.values()]), "  (largest Bucket ID)")
        print(sum([len((b.contents if isinstance(b.contents, Bucket) else eh.read_bucket(b.contents)).list) for _, b in eh.bucketPointers.items()]), " (total bucket items)")
        print(len([v for v in eh.bucketPointers.values() if isinstance(v.contents, Bucket)]), "(#buckets in-mem)")

