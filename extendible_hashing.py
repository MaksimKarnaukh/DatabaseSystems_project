from typing import List, Tuple, Dict, Union, Callable


class BucketValue(object):
    def __init__(self, key, value):
        self.key: str = key
        self.value: bytearray = value

    def __str__(self):
        return str(self.key) + " : " + str(self.value)

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value


class Bucket(object):
    amountBuckets = 0

    def __init__(self, local_prefix_size: int = 1, max_size: int = 2, bucket_values: List[BucketValue] = None, doIncrementID: bool=True):
        self.localPrefixSize: int = local_prefix_size
        self.maxSize: int = max_size

        self.list: List[BucketValue] = [] if bucket_values is None else bucket_values
        self.bucketID = Bucket.amountBuckets
        Bucket.amountBuckets += doIncrementID

    def __str__(self):
        result = f"<local {self.localPrefixSize}, maxSize {self.maxSize}> [\n"
        for elem in self.list:
            result += "    " + str(elem) + '\n'
        result += ']'
        return result

    def __len__(self):
        return len(self.list)

    def __eq__(self, other):
        return self.list == other.list

    def __bytes__(self):
         # make a bytearray of localPrefixSize, maxSize, bucketID, and then the bucketValues
        pass

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


def hash_function_str(key) -> str:
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
            # TODO: read bucket
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

    def insert_keyval_into_bucket(self, bucket: Bucket, keyHash: str, value):
        """Inserts a key-value pair into the index."""
        bucketValue: BucketValue = BucketValue(keyHash, value)
        success: bool = bucket.insert(bucketValue)
        # Bucket is full, split it
        if not success:
            self.split(bucket)
        success: bool = bucket.insert(bucketValue)
        assert (success is True, "After bucket split, insert MUST succeed")

    def insert_keyval(self, keyHash, value):
        """
        Inserts a key-value pair into the index.

        :param keyHash: hashed key
        :param value:
        :return:
        """
        bucket: Bucket = self.getBucket(keyHash=keyHash)
        self.insert_keyval_into_bucket(bucket=bucket, keyHash=keyHash, value=value)

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

        :param bucket: The bucket to split
        """
        # TODO: \/ Buckets are stored in pages in memory???? \/
        newBucketPointers: Dict[str: Bucket] = dict()
        for oldPrefix, oldBucket in self.bucketPointers.items():
            newPrefix0, newPrefix1 = self.get_extended_prefixes(oldPrefix)
            newBucketPointers[newPrefix0] = oldBucket
            newBucketPointers[newPrefix1] = oldBucket
        self.bucketPointers = newBucketPointers

        res0, res1 = self.split_bucket(bucket)
        newBucket0, newBucketPrefix0 = res0
        newBucket1, newBucketPrefix1 = res1
        self.bucketPointers[newBucketPrefix0] = newBucket0
        self.bucketPointers[newBucketPrefix1] = newBucket1
        # TODO: /\ READ ABOVE /\

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

        new_bucket0 = Bucket(local_prefix_size=bucket.localPrefixSize + 1)
        new_bucket1 = Bucket(local_prefix_size=bucket.localPrefixSize + 1)

        for i in range(len(bucketValues)):
            keyHash = bucketValues[i].get_key()[: len(newPrefix0)]

            if newPrefix0 == keyHash:
                new_bucket0.insert(BucketValue(key=keyHash, value=bucketValues[i].get_value()))
            elif newPrefix1 == keyHash:
                new_bucket1.insert(BucketValue(key=keyHash, value=bucketValues[i].get_value()))
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
        bucketFixedSize: int = 50
        with open("f", "rb") as f:
            f.seek(bucketFixedSize * bucketID)
            bucketBytes = f.read(bucketFixedSize)
        pass

    def write_bucket(self):
        pass


if __name__ == "__main__":


    if True
        eh = ExtendibleHashingIndex()
        for user_id in range(10):
            # bv = BucketValue(key=eh.get_hash_from_key(key=user_id), value=user_id)
            hashed_key = eh.get_hash_from_key(key=user_id)
            eh.insert_keyval(keyHash=hashed_key, value=user_id)

#        for bucket in eh.bucketPointers.values():
#            print(str(bucket))
