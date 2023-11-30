def get_hash_from_key(key):
    binary_string = bin(key)[2:]
    # Ensure the binary string has 32 bits by left-padding with zeros if needed
    padded_binary_string = binary_string.zfill(32)
    result = '0b' + padded_binary_string
    return result

# print(get_hash_from_key(30))

import struct

def int_to_32bit_binary_object(value):
    # Ensure the value is within the 32-bit range
    value &= 0xFFFFFFFF
    # Use struct.pack to convert the integer to a 4-byte binary object
    binary_object = struct.pack('<I', value)  # '<' specifies little-endian byte order
    return binary_object

# Example usage
original_value = 42
binary_representation = int_to_32bit_binary_object(original_value)

print(f"Original Value            : {original_value}")
print(f"32-bit Binary Representation: {binary_representation}")