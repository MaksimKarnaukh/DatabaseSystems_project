from typing import List, Tuple, Dict, Union, Callable


class BucketValue(object):
    def __init__(self, key, value):
        self.key: str = key
        self.value: bytearray = value

    def __str__(self):
        return str(self.key) + " : " + str(self.value)

    def __repr__(self):
        return self.__str__()

    def __bytes__(self):
        """
        Convert BucketValue to a byte string of length 20 bytes
        :return: bytestring of length 20
        """
        # make a bytearray of key and value
        key_integer = eval('0b' + self.key)
        key_bytes = key_integer.to_bytes(4, byteorder='big')
        value_bytes = self.value
        return bytearray(key_bytes + value_bytes)

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
        return cls(key_binary_string, bytearray(value_bytes))

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value


class Bucket(object):
    amountBuckets = 0

    def __init__(self, local_prefix_size: int = 1, max_size: int = 10, bucket_values: List[BucketValue] = None, doIncrementID: bool=True, manual_id: int = -1):
        self.localPrefixSize: int = local_prefix_size
        self.maxSize: int = max_size

        self.list: List[BucketValue] = [] if bucket_values is None else bucket_values

        if manual_id != -1:
            self.bucketID = manual_id
        else:
            self.bucketID = Bucket.amountBuckets
            Bucket.amountBuckets += doIncrementID

    def __str__(self):
        result = f"<local {self.localPrefixSize}, maxSize {self.maxSize}> [\n"
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
        # make a bytearray of localPrefixSize, maxSize, bucketID, and then the bucketValues.
        # in total this is: 1+1+4+10*20 = 206 bytes for one bucket
        local_prefix_size_bytes = self.localPrefixSize.to_bytes(1, byteorder='big')  # never a value > 32
        max_size_bytes = self.maxSize.to_bytes(1, byteorder='big')  # never a value > 255
        bucket_id_bytes = self.bucketID.to_bytes(4, byteorder='big')  # max 2^32 buckets
        bucket_values_bytes: bytearray = bytearray()
        for bucket_value in self.list:
            bucket_values_bytes += bytes(bucket_value)
        return bytearray(local_prefix_size_bytes + max_size_bytes + bucket_id_bytes + bucket_values_bytes)

    @classmethod
    def from_bytes(cls, byte_data):

        # Extract values from the byte_data
        local_prefix_size = int.from_bytes(byte_data[0:1], byteorder='big')
        max_size = int.from_bytes(byte_data[1:2], byteorder='big')
        bucket_id = int.from_bytes(byte_data[2:6], byteorder='big')

        bucket_values = []
        for i in range(6, len(byte_data), 20):
            key_bytes = byte_data[i:i + 4]
            value_bytes = byte_data[i + 4:i + 20]

            key = bin(int.from_bytes(key_bytes, byteorder='big'))[2:].zfill(32)
            value = bytearray(value_bytes)

            bucket_values.append(BucketValue(key, value))

        # Create and return the Bucket object
        return cls(local_prefix_size, max_size, bucket_values, doIncrementID=False, manual_id=bucket_id)

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
        :param value: value to insert
        :return: True if the value was inserted, False otherwise
        """
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


class ExtendibleHashingIndex(object):
    def __init__(self):

        self.bucketPointers: Dict[str: Union[int, Bucket]] = {
            "0": Bucket(),
            "1": Bucket()
        }
        self.globalHashPrefixSize: int = 1

        self.maxBucketInMemory: int = 6

    def __str__(self):
        reversedDict = dict()
        bucketByID = dict()
        for k, v in self.bucketPointers.items():
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

    def get_bucket(self, prefix: str):
        """

        :param prefix:
        :return:
        """
        bucket: Union[int, Bucket] = self.bucketPointers[prefix]
        if isinstance(bucket, int):
            # TODO: read bucket!
            # TODO: if #bucketsInMem > maxBucketsInMem then swap one bucket
            bucket = self.read_bucket()
        return bucket

    def set_bucket(self, prefix: str, bucket: Bucket):
        """

        :param prefix:
        :param bucket:
        :return:
        """
        # TODO: if #bucketsInMem > maxBucketsInMem then evict one bucket
        # TODO: write bucket
        self.bucketPointers[prefix] = bucket

    def get_hash_from_key(self, key, hash_function: Callable=hash_function_str):
        return hash_function(key)

    def getBucket(self, keyHash: str) -> Union[Bucket, None]:
        clippedPrefix: str = get_hash_prefix(keyHash=keyHash, prefixSize=self.globalHashPrefixSize)
        return self.bucketPointers.get(clippedPrefix, None)
    
    def get(self, key):
        """
        Returns the first item with the given key from the index.
        :param key: non-hashed key
        :return:
        """
        # first, get the bucket associated with the key
        keyHash = self.get_hash_from_key(key=key)
        bucket = self.getBucket(keyHash=keyHash)
        # then, get the item from the bucket
        return bucket.search(keyHash)

    def insert_bucket(self, bucket: Bucket, clipped_prefix: str):
        """Inserts a key-value pair into the index."""
        pass

    def insert_keyval(self, keyHash: str, value):
        """Inserts a key-value pair into the index."""
        bucket: Bucket = self.getBucket(keyHash=keyHash)
        bucketValue: BucketValue = BucketValue(keyHash, value)
        success: bool = bucket.insert(bucketValue)

        # Bucket is full, split it
        if not success:
            self.split(bucket)

            # insert recursively (for in the case that the destination bucket is still full)
            self.insert_keyval(keyHash, value)

    def delete(self, key):
        """
        Deletes the first item with the given key from the index.
        :param key: non-hashed key
        :return:
        """
        # first, get the bucket associated with the key
        keyHash = self.get_hash_from_key(key=key)
        bucket = self.getBucket(keyHash=keyHash)
        # then, delete the item from the bucket
        return bucket.delete(keyHash)

    def split(self, bucket: Bucket) -> None:
        """Perfom a split on the index for a given bucket.
        Perform any necessary actions after the split to
        bring the index into a valid state again.

        IMPORTANT: This method invalidates the passed *bucket* reference

        :param bucket: The bucket to split
        """
        # TODO: \/ Buckets are stored in pages in memory???? \/
        shouldIncreaseGlobal: bool = bucket.localPrefixSize == self.globalHashPrefixSize

        if shouldIncreaseGlobal:
            newBucketPointers: Dict[str: Union[Bucket, int]] = dict()
            for oldPrefix, oldBucket in self.bucketPointers.items():
                newPrefix0, newPrefix1 = self.get_extended_prefixes(oldPrefix)
                newBucketPointers[newPrefix0] = oldBucket
                newBucketPointers[newPrefix1] = oldBucket
            self.bucketPointers = newBucketPointers

        res0, res1 = self.split_bucket(bucket)
        newBucket0, newBucketPrefix0 = res0
        newBucket1, newBucketPrefix1 = res1

        # because we did a split, we need to update all related pointers
        for ptr in self.bucketPointers.keys():
            if ptr.startswith(newBucketPrefix0):
                self.bucketPointers[ptr] = newBucket0
            elif ptr.startswith(newBucketPrefix1):
                self.bucketPointers[ptr] = newBucket1

        self.globalHashPrefixSize += shouldIncreaseGlobal

    def split_bucket(self, bucket: Bucket):
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

        new_bucket0 = Bucket(local_prefix_size=bucket.localPrefixSize + 1, doIncrementID=False)
        new_bucket0.bucketID = bucket.bucketID      # Do not waste unused ID
        new_bucket1 = Bucket(local_prefix_size=bucket.localPrefixSize + 1)

        for idx, bucketValueObj in enumerate(bucketValues):
            bucketKey = bucketValueObj.get_key()
            keyHash = bucketKey[: len(newPrefix0)]

            if newPrefix0 == keyHash:
                new_bucket0.insert(bucketValueObj)
            elif newPrefix1 == keyHash:
                new_bucket1.insert(bucketValueObj)
            else:
                print("YOU SHOULDN'T EVER SEE THIS MESSAGE")

        return [new_bucket0, newPrefix0], [new_bucket1, newPrefix1]

    def get_extended_prefixes(self, prefix: str) -> Tuple[str, str]:
        """Convert the prefix into two extended prefixes of one 'bit' longer.

        :return: (
            prefix | '0',
            prefix | '1'
        )
        """
        return prefix + '0', prefix + '1'

    def read_bucket(self, bucketID):
        bucketFixedSize: int = 206
        with open("f", "rb") as f:
            f.seek(bucketFixedSize * bucketID)
            bucketBytes = f.read(bucketFixedSize)
            
            b = Bucket.from_bytes(bucketBytes)


    def write_bucket(self, bucket: Bucket):
        """
        Write the bytes of the bucket to a file.
        :param bucket: bucket object
        :return:
        """
        bucket_record_size = 206

        with open("buckets_data.dat", "rb+") as file:
            bucket_bytes = bytes(bucket)

            if len(bucket_bytes) > bucket_record_size:
                raise ValueError("Bucket data size exceeds the specified record size.")

            # If the bucket data is smaller than the record size, pad it with zeros
            padded_bucket_bytes = bucket_bytes + bytes([0] * (bucket_record_size - len(bucket_bytes)))

            assert len(padded_bucket_bytes) == bucket_record_size

            file.seek(bucket_record_size * bucket.bucketID)
            file.write(padded_bucket_bytes)

    def getViolations(self, exitOnViolation: bool=True) -> list:
        """Check whether the ExtendibleHashingIndex is valid.

        :return: A list of all violations
        """
        violations = []
        for prefix, bucket in self.bucketPointers.items():
            # A prefix is of the correct size
            isLenCorrect = len(prefix) == self.globalHashPrefixSize
            if not isLenCorrect:
                violations.append(f"bad prefix length: found {len(prefix)}, expected {self.globalHashPrefixSize}")
                if exitOnViolation:
                    return violations

            element: BucketValue
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

    if True:
        import random
        data_set = [i for i in range(10000)] # list(range(30))
        random.shuffle(data_set)

        # insert 10000 entries in a random order
        for user_id in data_set:
            hashed_key = eh.get_hash_from_key(key=user_id)
            eh.insert_keyval(keyHash=hashed_key, value=user_id)

        # TESTING
        print(eh)
        print(eh.isValid())
        print(eh.getViolations(False))

