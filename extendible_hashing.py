from typing import List, Union, Callable


class BucketValue(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return str(self.key) + " : " + str(self.value)

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value


class Bucket(object):
    def __init__(self,local_prefix_size: int = 1, max_size: int = 2, bucket_values: List[BucketValue] = None):
        self.localPrefixSize: int = local_prefix_size
        self.max_size: int = max_size

        self.list: List[BucketValue] = [] if bucket_values is None else bucket_values

    def __str__(self):
        return str(self.list)

    def __len__(self):
        return len(self.list)


    def get_local_prefix_size(self) -> int:
        return self.localPrefixSize

    def set_local_prefix_size(self, size: int):
        self.localPrefixSize = size

    def get_max_size(self) -> int:
        return self.max_size

    def get_bucket_values(self) -> List[BucketValue]:
        return self.list

    def insert(self, value: BucketValue) -> bool:
        """
        Inserts a value of type BucketValue into the bucket.
        :param value: value to insert
        :return: True if the value was inserted, False otherwise
        """
        if len(self.list) < self.max_size:
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


def hash_function_bin(key):
    """Hashes a key and returns the hash value."""
    # binary_string = bin(key)[2:]
    # # Ensure the binary string has 32 bits by left-padding with zeros if needed
    # padded_binary_string = binary_string.zfill(32)
    # result = '0b' + padded_binary_string[::-1]
    # result = bytes(result, 'utf-8')
    # return result
    return key.to_bytes(4, 'big')

def get_clipped_prefix(keyHash: bytes, localPrefixSize: int) -> int:
    keyHashLenFeremans: int = len(keyHash) * 8  # Convert to Bytes to bits
    shiftAmount: int = keyHashLenFeremans - localPrefixSize
    clippedPrefix: int = int.from_bytes(keyHash, 'big') >> shiftAmount
    return clippedPrefix


class ExtendibleHashingIndex(object):
    def __init__(self):

        self.bucketPointers: dict[int: Bucket] = {
            0: Bucket(),
            1: Bucket()
        }
        self.globalHashPrefixSize: int = 1


    def get_hash_from_key(self, key, hash_function: Callable=hash_function_bin):
        return hash_function(key)

    def getBucket(self, keyHash: bytes) -> Union[Bucket, None]:
        keyHashLenFeremans: int = len(keyHash) * 8  # Convert to Bytes to bits
        shiftAmount: int = keyHashLenFeremans - self.globalHashPrefixSize
        clippedPrefix: int = int.from_bytes(keyHash, 'big') >> shiftAmount
        return self.bucketPointers.get(clippedPrefix, None)

    def insert_bucket(self, bucket, clipped_prefix):
        """Inserts a key-value pair into the index."""
        pass

    def insert_keyval_into_bucket(self, bucket: Bucket, key, value):
        """Inserts a key-value pair into the index."""
        success: bool = bucket.insert(BucketValue(key, value))
        if not success:
            # Bucket is full, split it
            new_bucket1, new_bucket2 = self.split(bucket)
            self.insert_bucket(*new_bucket1)
            self.insert_bucket(*new_bucket2)

    def insert_keyval(self, keyHash, value):
        """
        Inserts a key-value pair into the index.
        :param keyHash: hashed key
        :param value:
        :return:
        """
        if value == 1:
            a = 2
        bucket: Bucket = self.getBucket(keyHash=keyHash)
        self.insert_keyval_into_bucket(bucket=bucket, key=keyHash, value=value)

    def delete(self, key):
        pass

    def search(self, key):
        pass

    def split(self, bucket: Bucket):

        bucketValues = bucket.get_bucket_values()
        randomBucketHash = bucketValues[0].get_key().to_bytes(4)
        clipped_prefix = get_clipped_prefix(randomBucketHash, bucket.localPrefixSize)

        b1 = clipped_prefix*2
        b2 = clipped_prefix*2 + 1

        binary_string = bin(b1)[2:]
        padded_binary_string_b1 = binary_string.zfill(32)

        binary_string = bin(b2)[2:]
        padded_binary_string_b2 = binary_string.zfill(32)

        new_bucket1 = Bucket(local_prefix_size=bucket.localPrefixSize + 1)
        new_bucket2 = Bucket(local_prefix_size=bucket.localPrefixSize + 1)

        for i in range(len(bucketValues)):
            keyHash = bucketValues[i].get_key().to_bytes(4)

            if padded_binary_string_b1 & keyHash == keyHash:
                new_bucket1.insert(BucketValue(key=keyHash, value=bucketValues[i].get_value()))
            elif padded_binary_string_b2 & keyHash == keyHash:
                new_bucket2.insert(BucketValue(key=keyHash, value=bucketValues[i].get_value()))
            else:
                print("YOU SHOULDN'T EVER SEE THIS MESSAGE")

        return [new_bucket1, b1], [new_bucket2, b2]


if __name__ == "__main__":
    eh = ExtendibleHashingIndex()
    for user_id in range(10):
        # bv = BucketValue(key=eh.get_hash_from_key(key=user_id), value=user_id)
        hashed_key = eh.get_hash_from_key(key=user_id)
        eh.insert_keyval(keyHash=hashed_key, value=user_id)





# HASH_SIZE: int = 32      # Key hash size in bits
# globalHashPrefixSize: int = 4
# < bucketLocalSize: List[ bucket ] >
# bucketsList: dict[int: Bucket] = {
#     "0": Bucket(),
#     "1": Bucket()
# }

# hashValue = 0b10100000_00000000_00000000_00000000.to_bytes(4, 'big')
# print(str(hashValue))
#hashValue = hashValue[0:3]

# def getBucket(keyHash: bytes) -> Union[Bucket, None]:
#     keyHashLenFeremans: int = len(keyHash) * 8  # Convert to Bytes to bits
#     shiftAmount: int = keyHashLenFeremans - globalHashPrefixSize
#     clippedPrefix: int = int.from_bytes(keyHash, 'big') >> shiftAmount
#     a = 2
#     return bucketsList.get(clippedPrefix, None)


# print(getBucket(hashValue))




