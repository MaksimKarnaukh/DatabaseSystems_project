# import bplustree

import pandas as pd
from IPython.display import display
from extendible_hashing import ExtendibleHashingIndex, BucketValue
from typing import Union
import copy

df = pd.DataFrame(
    columns=['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country_dct',
             'birthdate_ts'])
df = df[['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country_dct', 'birthdate_ts']]
new_user_columns = list(df.columns.values)
print(new_user_columns)
#some constants for efficiency
IDX_ID = new_user_columns.index('id')
IDX_SN = new_user_columns.index('street_number')
IDX_ZIP = new_user_columns.index('zipcode')
IDX_BD = new_user_columns.index('birthdate_ts')
IDX_COUNTRY = new_user_columns.index('country_dct')
IDX_NAME = new_user_columns.index('name')
IDX_EMAIL = new_user_columns.index('email')
IDX_PHONE = new_user_columns.index('phone')
IDX_COMPANY = new_user_columns.index('company')
IDX_STREET = new_user_columns.index('street')

# Assume a page is 8192B
PAGE_SIZE: int = 8192
# There can be at most 2^13 = 8192 tuples in a page.
TUPLE_CTR_SIZE: int = 2
# 2B gives a max offset value of 2^13 = 8192,
# meaning that all tuples may be 1B and will still
# be addressable with an offset.
OFFSET_SIZE: int = 2

# An index for the user's ids in the form of a Hashtable.
# The mapping is as follows:
#       user_ID : bytearray(page_idx | slot_address)
# where user_ID is the identifier in the user's id column
# and the bytearray consists of the page's index where the
# user tuple is stored padded to 8B and the slot_address
# padded to 8B, which is the offset within the page to
# the slot corresponding to the user tuple.
user_index: ExtendibleHashingIndex = ExtendibleHashingIndex()
remaining_page_mem_index = dict()


def encode_var_string(s):
  return [len(s)] + list(s.encode('ascii'))

def encode_user_var_length(user, is_new_user=False):
  '''
  Assuming user has columns
  ['id', 'name', 'email', 'phone', 'company', 'street', 'street_number', 'zipcode', 'country_dct', 'birthdate_ts']

  encode user object:
  id, street_number, zipcode, birthdate_ts, country_dct
    -> to integer between 1 and 4 bytes depending on range values
  name, email, phone, company, street
    -> to variable-length string, e.g. "helloworld" -> (8,"helloworld") instead of using padding, e.g."0000000helloworld"
  '''
  if not is_new_user:
      int_list = []
      int_list.extend(int(user[IDX_ID]).to_bytes(4,'little'))
      int_list.extend(int(user[IDX_SN]).to_bytes(2,'little')) #max street number < 65536 (or 2^16)
      int_list.extend(int(user[IDX_ZIP]).to_bytes(4,'little'))
      int_list.extend(int(user[IDX_BD]).to_bytes(4,'little'))
      int_list.extend(int(user[IDX_COUNTRY]).to_bytes(1,'little')) #max country < 256 (or 2^8)
      int_list.extend(encode_var_string(user[IDX_NAME]))
      int_list.extend(encode_var_string(user[IDX_EMAIL]))
      int_list.extend(encode_var_string(user[IDX_PHONE]))
      int_list.extend(encode_var_string(user[IDX_COMPANY]))
      int_list.extend(encode_var_string(user[IDX_STREET]))
      return bytearray(int_list)
  else: # example user input: [1, 'A', 'A@m.c', '1', 'G', 'addr', 1, 2, 1, 1234567890]
      int_list = []
      int_list.extend(int(user[0]).to_bytes(4, 'little'))
      int_list.extend(int(user[6]).to_bytes(2, 'little'))  # max street number < 65536 (or 2^16)
      int_list.extend(int(user[7]).to_bytes(4, 'little'))
      int_list.extend(int(user[9]).to_bytes(4, 'little'))
      int_list.extend(int(user[8]).to_bytes(1, 'little'))  # max country < 256 (or 2^8)
      int_list.extend(encode_var_string(user[1]))
      int_list.extend(encode_var_string(user[2]))
      int_list.extend(encode_var_string(user[3]))
      int_list.extend(encode_var_string(user[4]))
      int_list.extend(encode_var_string(user[5]))
      return bytearray(int_list)

def decode_user_var_length(byte_array):
    '''
    decode variable-length tuple representing user (see encode_user_var_length)
    '''
    id = int.from_bytes(byte_array[0:4], byteorder='little')
    street_number = int.from_bytes(byte_array[4:6], byteorder='little')
    zipcode = int.from_bytes(byte_array[6:10], byteorder='little')
    bd = int.from_bytes(byte_array[10:14], byteorder='little')
    country_dct = int.from_bytes(byte_array[14:15], byteorder='little')

    name_len = int.from_bytes(byte_array[15:16], "little")
    name = byte_array[16:16 +name_len].decode('ascii')
    email_len = int.from_bytes(byte_array[16 +name_len:16 +name_len +1], "little")
    email = byte_array[16 +name_len +1:16 +name_len + 1 +email_len].decode('ascii')
    phone_len = int.from_bytes(byte_array[16 +name_len + 1 +email_len:16 +name_len + 1 +email_len +1], "little")
    phone = byte_array[16 +name_len + 1 +email_len +1:16 +name_len + 1 +email_len + 1 +phone_len].decode('ascii')
    company_len = int.from_bytes(byte_array[16 +name_len + 1 +email_len + 1 +phone_len:16 +name_len + 1 +email_len + 1 +phone_len +1], "little")
    company = byte_array[16 +name_len + 1 +email_len + 1 +phone_len +1:16 +name_len + 1 +email_len + 1 +phone_len + 1 +company_len].decode('ascii')
    street_len = int.from_bytes(byte_array[16 +name_len + 1 +email_len + 1 +phone_len + 1 +company_len:16 +name_len + 1 +email_len + 1 +phone_len + 1 +company_len +1], "little")
    street = byte_array[16 +name_len + 1 +email_len + 1 +phone_len + 1 +company_len +1:16 +name_len + 1 +email_len + 1 +phone_len + 1 +company_len + 1 +street_len].decode('ascii')

    l = [id, name, email, phone, company, street, street_number, zipcode, country_dct, bd]

    return l


class Page:
    def __init__(self, page_size: int, tuple_ctr_size: int, slot_size: int):
        """
        Initialize an empty page. A page has the following structure as a bytearray:
        [page_size offset_ptr1 offset_ptr2 ... tuple_2 tuple_1]

        :param page_size: The size of the page in bytes
        :param tuple_ctr_size: The size of the page's tuple counter in bytes
        :param slot_size: The size of an offset ptr in bytes
        """
        # Page constants
        self.page_size: int = page_size
        self.tuple_ctr_size: int = tuple_ctr_size
        self.slot_size: int = slot_size

        # Page contents
        # Initialized with null bytes
        self.bytearray: bytearray = bytearray(page_size)

        # Page variable members
        # Tuples grow from back to front in the page
        self.tuples_data_base_address: int = self.page_size

    @property
    def slot_array(self) -> bytearray:
        """Extract the slot array from the page. The slot
         array contains the addresses of the tuples corresponding
         to each slot as an offset within the page's bytearray.

         The size of each slot is specified by Page.slot_size,
         so each subsequent series of Page.slot_size bytes in
         the return value is a single slot.

        :return: A bytearray the slot array
        """
        return self.bytearray[self.tuple_ctr_size: self.tuple_ctr_size + self.tuple_count * self.slot_size]

    @property
    def tuples_data(self) -> bytearray:
        """Extract the tuples data from the page.

        :return: A bytearray containing the tuples data
        """
        return self.bytearray[self.tuples_data_base_address:]

    @property
    def tuple_count(self) -> int:
        """Extract the current tuple count from the page bytes.

        :return: The up-to-date tuple count as an int
        """
        return int.from_bytes(self.bytearray[0: self.tuple_ctr_size], byteorder='little')

    @property
    def unused_memory_size(self) -> int:
        """Determine the amount of unused memory inside the page in bytes.
        
        :return: Unused memory in bytes as an int
        """
        return self.tuples_data_base_address - self.tuple_ctr_size - self.tuple_count * self.slot_size
    
    def set_tuple_count(self, new_count: int) -> bool:
        assert new_count >= 0, "Cannot set a Page's tuple count to a negative value."

        new_ctr_bytes: bytes = new_count.to_bytes(self.tuple_ctr_size, byteorder='little')
        self.bytearray[0: self.tuple_ctr_size] = new_ctr_bytes

        return True

    def load_bytes(self, page_bytes: bytearray):
        """Load the specified bytearray into the Page instance, overwriting
         the page's current bytearray.

        Returns self to allow chaining.

        :param page_bytes: The bytearray to load
        :return: The page instance (self)
        """
        assert len(page_bytes) == self.page_size, f"Invalid data size: expected {self.page_size}B, got {len(page_bytes)}B"
        self.bytearray = page_bytes

        self.tuples_data_base_address = self.page_size
        if self.tuple_count > 0:
            self.tuples_data_base_address = self.get_tuple_address(self.get_slot_address(-1))

        return self

    def is_valid_slot_address(self, slot_address: int) -> bool:
        """Check whether the specified slot address is a valid slot address.

        :param slot_address: The address to validate
        :return: The validation result, True if valid, else False
        """
        above_lower_bound: bool = slot_address >= self.tuple_ctr_size
        below_upper_bound: bool = slot_address <= self.tuple_ctr_size + (self.tuple_count - 1) * self.slot_size
        multiple_of_slot_size: bool = (slot_address % self.slot_size) == 0
        return above_lower_bound and below_upper_bound and multiple_of_slot_size

    def get_slot_address(self, slot_index: int) -> int:
        """Convert the specified slot index to the address of the corresponding
         slot in the slot array. Negative indexes wrap around. Index zero corresponds
         to the first slot in the array.

        :param slot_index: The index of the slot to get the slot address for
        :return: The slot address as an int
        """
        # modulo on a negative int wraps around
        bounded_index: int = slot_index % self.tuple_count
        return self.tuple_ctr_size + bounded_index * self.slot_size

    def get_tuple_address(self, slot_address: int) -> int:
        """Get the address of the tuple stored at the specified slot address.
        The specified slot address is required to be a valid slot address,
        meaning that it does not exceed the slot array's size as specified
        by the tuple count.

        :param slot_address: The slot address to extract the tuple address from
        :return: The tuple address
        """

        assert self.is_valid_slot_address(slot_address), "Invalid slot address! Cannot get the tuple address."

        tuple_address_bytes: bytearray = self.bytearray[slot_address: slot_address + self.slot_size]
        return int.from_bytes(tuple_address_bytes, byteorder='little')

    def append_tuple(self, tuple_bytes: bytearray) -> int:
        """
        Append a tuple to the page. Requires the page to have enough free space.

        :param tuple_bytes: The tuple bytes to append to the page
        :return: The address of the newly stored tuple as an offset within this page
        """
        assert self.data_fits(tuple_bytes), f"Page is full, cannot write tuple bytes: {tuple_bytes}"

        # Setup
        init_tuple_count: int = self.tuple_count

        # write user to page
        prev_tuples_base_address = self.tuples_data_base_address
        self.tuples_data_base_address -= len(tuple_bytes)
        self.bytearray[self.tuples_data_base_address: prev_tuples_base_address] = tuple_bytes

        # write offset to page
        new_slot_address = self.tuple_ctr_size + init_tuple_count * self.slot_size
        self.bytearray[new_slot_address: new_slot_address + self.slot_size] =\
            self.tuples_data_base_address.to_bytes(self.slot_size, 'little')
        self.set_tuple_count(init_tuple_count + 1)

        return new_slot_address

    def remove_tuple(self, user_id: int, page_number: int, del_user_slot_address: int) -> None:
        """Remove the tuple corresponding to the *del_user_slot_address* slot from the page.
        Also updates the user_index and remaining_page_mem_index indexes.

        :param user_id:
        :param page_number: The index of the page
        :param del_user_slot_address: The slot address corresponding to the tuple to remove
        """
        from typing import List

        initial_tuple_count: int = self.tuple_count
        last_tuple_address: int = self.get_tuple_address(self.get_slot_address(-1))
        del_user_address: int = self.get_tuple_address(del_user_slot_address)
        del_tuple_size: int = 0

        # Start of slot array
        if del_user_slot_address == TUPLE_CTR_SIZE:
            del_tuple_size = self.page_size - del_user_address
        # Not start of slot array
        else:
            prev_slot_address: int = del_user_slot_address - self.slot_size
            prev_tuple_address: int = self.get_tuple_address(prev_slot_address)
            del_tuple_size = prev_tuple_address - del_user_address

        to_move_tuples_amount: int = initial_tuple_count - (
                (del_user_slot_address - self.tuple_ctr_size) // self.slot_size + 1)
        next_slot_address: int = del_user_slot_address + self.slot_size
        updated_slot_contents_list: List[int] = []

        # update slot array contents/values in page
        for to_move_slot_address in range(next_slot_address, next_slot_address + to_move_tuples_amount * self.slot_size,
                                          self.slot_size):
            original_slot_contents: int = self.get_tuple_address(to_move_slot_address)
            updated_slot_contents: int = original_slot_contents + del_tuple_size
            updated_slot_contents_list.append(updated_slot_contents)

            updated_slot_contents_bytes: bytes = updated_slot_contents.to_bytes(self.slot_size, byteorder='little')
            self.bytearray[to_move_slot_address - self.slot_size: to_move_slot_address] = updated_slot_contents_bytes

        user_data = self.bytearray[last_tuple_address: del_user_address]

        # The differences between slot contents, meaning they are the tuple lengths
        tuple_lengths: List[int] = [
            updated_slot_contents_list[idx] - updated_slot_contents_list[idx + 1]
            for idx in range(0, len(updated_slot_contents_list) - 1)
        ]
        # The loop iterates from the leftmost tuple in the user_data,
        # but the leftmost slot in the updated_slot_contents_list corresponds
        # to the rightmost tuple in user data, so reverse tuple_lengths.
        # Add random last value so that every offset is iterated on
        tuple_lengths = tuple_lengths[::-1] + [0]
        # The new/updated slot addresses corresponding to the moved/shifted tuples.
        updated_slot_addresses: List[int] = \
            [
                slot_address - self.slot_size
                for slot_address in
                range(del_user_slot_address + to_move_tuples_amount * self.slot_size, del_user_slot_address,
                      -self.slot_size)
            ]

        progress: int = 0
        for idx, updated_slot_address in enumerate(updated_slot_addresses):
            tuple_len: int = tuple_lengths[idx]
            user_id_size: int = 4
            user_id_bytes: bytes = user_data[progress: progress + user_id_size]
            progress += tuple_len

            user_id_int: int = int.from_bytes(user_id_bytes, 'little')
            # Slot addresses may be shorter than the space allocated to them in the index
            padded_updated_slot_address: bytes = updated_slot_address.to_bytes(8, byteorder='little')
            user_index.insert_keyval(user_id_int, page_number.to_bytes(8, byteorder='little') + padded_updated_slot_address)
            # user_index[user_id_int] = page_number.to_bytes(8, byteorder='little') + padded_updated_slot_address

        # Shift tuples to eliminate fragmentation due to single tuple delete
        data_shift_address: int = last_tuple_address + del_tuple_size
        self.bytearray[data_shift_address: data_shift_address + len(user_data)] = user_data
        self.tuples_data_base_address += del_tuple_size

        # Update tuple counter
        self.set_tuple_count(initial_tuple_count - 1)

        # update user index
        user_index.delete(user_id)
        # del user_index[user_id]

        freed_memory: int = self.slot_size + del_tuple_size
        remaining_page_mem_index[page_number] = remaining_page_mem_index.get(page_number, 0) + freed_memory

    def data_fits(self, tuple_data: bytearray) -> bool:
        """
        Whether the page contains enough free space to store the *tuple_data*.

        :param tuple_data: The data to compare against available space
        :return: True if enough space available, else False
        """
        # == The space allocated to the offset pointer array
        allocated_offsetptr_space: int = self.tuple_count * self.slot_size
        # All page space after the offset has been allocated to tuples.
        # == free space in page
        free_page_space: int = self.tuples_data_base_address - (self.tuple_ctr_size + allocated_offsetptr_space)
        # Adding a tuple requires space for the tuple and an offset ptr
        required_tuple_space: int = len(tuple_data) + self.slot_size
        return free_page_space >= required_tuple_space


def create_empty_page() -> Page:
    return Page(PAGE_SIZE, TUPLE_CTR_SIZE, OFFSET_SIZE)


def save_users_to_binary_var_length(filename, df):
    """
    saves users to fixed-length pages (our file is split up into blocks of fixed length (= pages), in this case 8192 bytes that each can contain a certain number of users)
    we also make a bplustree with key = user id and value = page number and offset of the user in that page to be able to quickly find a user by id
    file layout: [page1 page2 ... page_N]
    page layout: [N offset_t1 offset_t2... offset_tN offset_tN+1 t1 t2 ... tN]

    :param filename: binary file to save
    :param df: pandas dataframe contains all users
    :return:
    """
    from typing import List

    # create pages
    pages: List[Page] = []
    page: Page = create_empty_page()
    pages.append(page)

    for index, row in df.iterrows():
        user = encode_user_var_length(row)

        if not page.data_fits(user):
            page: Page = create_empty_page()
            pages.append(page)

        # write user to page
        new_offset_address: int = page.append_tuple(user)

        # add key = user id, value = page number and offset of user in that page to bplustree
        user_index.insert_keyval(row['id'], (len(pages) - 1).to_bytes(8, byteorder='little') + new_offset_address.to_bytes(8, byteorder='little'))
        # user_index[row['id']] = (len(pages) - 1).to_bytes(8, byteorder='little') + new_offset_address.to_bytes(8, byteorder='little')

    # note how much free space there is left per page
    for idx, p in enumerate(pages):
        allocated_offsetptr_space: int = p.tuple_count * p.slot_size
        free_page_space: int = p.tuples_data_base_address - (p.tuple_ctr_size + allocated_offsetptr_space)
        remaining_page_mem_index[idx] = free_page_space

    with open(filename, "wb") as f:
        # write pages to file
        for page in pages:
            f.write(page.bytearray)

    f.close()


def load_users_from_binary_var_length(filename):
    """
    load users from pages
    page layout: [N offset_t1 offset_t2... offset_tN offset_tN+1 tN ...t2 t1]

    :param filename: binary file to load
    :return: pandas dataframe contains all users
    """

    # extract users
    users = []
    with open(filename, "rb") as f:
        # iterate over pages
        page = f.read(PAGE_SIZE)
        while page:
            # get number of users in page
            nr_users_in_page = int.from_bytes(page[0:TUPLE_CTR_SIZE], 'little')

            # iterate over users
            for i in range(nr_users_in_page):
                # get offset of user
                offset_offset = TUPLE_CTR_SIZE + i*OFFSET_SIZE
                offset = int.from_bytes(page[offset_offset:offset_offset + OFFSET_SIZE], 'little')

                user_size = 0
                if i == 0:
                    user_size = PAGE_SIZE - offset
                else:
                    prev_offset = int.from_bytes(page[offset_offset - OFFSET_SIZE:offset_offset], 'little')
                    user_size = prev_offset - offset

                # get user
                user = page[offset:offset + user_size]
                # decode user
                users.append(decode_user_var_length(user))

            # read next page
            page = f.read(PAGE_SIZE)

    df = pd.DataFrame(users, columns=new_user_columns)
    return df


def read_var_length_user(db_filename: str, user_id: int):
    """
    Perform a random read for the user uniquely identified by the *user_id*.
    we first find the right page, then offset and get the user.

    :param db_filename: The file name of the database file
    :param user_id: The user id of the tuple to retrieve.
    :param user_index: bplustree index
    :return: The user data
    """
    tuple_location: bytes = bytes()
    found: Union[BucketValue, None] = user_index.get(user_id)
    if found is not None:
        tuple_location = found.value

    if tuple_location is None or len(tuple_location) == 0:
        return None
    page, offset_ptr = int.from_bytes(tuple_location[0:8], 'little'), int.from_bytes(tuple_location[8:16], 'little')
    with open(db_filename, "rb") as f:
        f.seek(page * PAGE_SIZE + offset_ptr)
        user_size = 0

        offset_in_page = f.read(TUPLE_CTR_SIZE)
        offset_in_page_int = int.from_bytes(offset_in_page, 'little')
        page_base_address: int = page * PAGE_SIZE

        if offset_ptr == TUPLE_CTR_SIZE:
            user_size = PAGE_SIZE - offset_in_page_int
        else:
            prev_offset_ptr: int = offset_ptr - OFFSET_SIZE

            f.seek(page_base_address + prev_offset_ptr)
            prev_offset_ptr = int.from_bytes(f.read(OFFSET_SIZE), 'little')
            user_size = prev_offset_ptr - offset_in_page_int

        f.seek(page_base_address + offset_in_page_int)
        user = f.read(user_size)
        return decode_user_var_length(user)


def get_page_with_enough_space(db_filename: str, user_size: int):
    """
    Get the page number of the page with enough space to store the user.

    :param db_filename: binary file
    :param user_size: size of user to add
    :return:
    """
    from typing import List

    # get page with enough space
    page_number = None
    for idx, free_space in remaining_page_mem_index.items():
        if free_space >= user_size + OFFSET_SIZE:
            page_number = idx
            break

    # if no page has enough space, create new page
    if page_number is None:
        page_number = len(remaining_page_mem_index)
        page: Page = create_empty_page()
        # write page to the end of the binary file
        with open(db_filename, "ab") as f:
            f.write(page.bytearray)
        f.close()

        remaining_page_mem_index[page_number] = PAGE_SIZE

    return page_number


def create_var_length_user(db_filename: str, user_tuple):
    """
    Create a new user tuple in the database.

    :param db_filename: binary file
    :param user_tuple: unencoded user tuple
    :return:
    """

    # get user id
    user_id = user_tuple[0]
    # check if user already exists
    assert user_index.get(user_id) is None, "user already exists"

    # get user
    encoded_user_tuple = encode_user_var_length(user_tuple)
    # get user size
    user_size = len(encoded_user_tuple)

    # get page with enough space
    page_number = get_page_with_enough_space(db_filename, user_size)

    # write encoded user tuple to page
    with open(db_filename, "r+b") as f:
        # get page
        page: Page = create_empty_page()
        f.seek(page_number * PAGE_SIZE)
        page.load_bytes(bytearray(f.read(page.page_size)))

        # write user to page
        new_offset_address: int = page.append_tuple(encoded_user_tuple)

        # add page and user offset to user index
        user_index.insert_keyval(user_id, page_number.to_bytes(8, 'little') + new_offset_address.to_bytes(8, 'little'))
        # user_index[user_id] = page_number.to_bytes(8, 'little') + new_offset_address.to_bytes(8, 'little')

        # write page to binary file
        f.seek(page_number * PAGE_SIZE)
        f.write(page.bytearray)

        # update remaining page mem index
        remaining_page_mem_index[page_number] -= user_size + OFFSET_SIZE


def delete_var_length_user(db_filename: str, user_id):
    """
    Delete a user tuple from the database.

    :param db_filename: The file containing the page the user is stored in
    :param user_id: The id column value for the user' db row
    """
    # Perform index lookup
    # tuple_location: bytes = user_index.get(user_id)

    tuple_location: bytes = bytes()
    found: Union[BucketValue, None] = user_index.get(user_id)
    if found is not None:
        tuple_location = found.value

    if tuple_location is None or len(tuple_location) == 0:
        return None
    page_number: int = int.from_bytes(tuple_location[0:8], 'little')
    del_user_slot_address: int = int.from_bytes(tuple_location[8:16], 'little')

    with open(db_filename, "r+b") as f:
        # Setup page
        page: Page = create_empty_page()
        f.seek(page_number * PAGE_SIZE)
        page.load_bytes(bytearray(f.read(page.page_size)))

        page.remove_tuple(user_id, page_number, del_user_slot_address)

        # Actually write page to memory
        f.seek(page_number * PAGE_SIZE)
        f.write(page.bytearray)


def update_var_length_user(db_filename: str, user_id, updated_user_tuple):
    """
    Update a user tuple in the database.

    :param db_filename: binary file
    :param user_id: user id of the user to update
    :param updated_user_tuple: unencoded user tuple
    :return:
    """
    # Perform index lookup
    # tuple_location: bytes = user_index.get(user_id)

    tuple_location: bytes = bytes()
    found: Union[BucketValue, None] = user_index.get(user_id)
    if found is not None:
        tuple_location = found.value

    if tuple_location is None or len(tuple_location) == 0:
        return None
    page_number: int = int.from_bytes(tuple_location[0:8], 'little')
    update_user_slot_address: int = int.from_bytes(tuple_location[8:16], 'little')
    final_page_number: int = -1

    encoded_updated_user_tuple = encode_user_var_length(updated_user_tuple)
    updated_user_tuple_size = len(encoded_updated_user_tuple)

    with open(db_filename, "r+b") as f:
        # Setup page
        page: Page = create_empty_page()
        f.seek(page_number * page.page_size)
        page.load_bytes(bytearray(f.read(page.page_size)))

        # Setup vars
        updated_user_address: int = page.get_tuple_address(update_user_slot_address)
        old_user_tuple_size: int = 0

        # Start of slot array
        if update_user_slot_address == TUPLE_CTR_SIZE:
            old_user_tuple_size = PAGE_SIZE - updated_user_address
        # Not start of slot array
        else:
            prev_slot_address: int = update_user_slot_address - page.slot_size
            prev_tuple_address: int = page.get_tuple_address(prev_slot_address)
            old_user_tuple_size = prev_tuple_address - updated_user_address

        # first check if there is enough space for the updated user tuple in the page if we would replace the old one
        if remaining_page_mem_index.get(page_number, 0) < updated_user_tuple_size - old_user_tuple_size:
            # not enough space
            # delete old user tuple from page
            page.remove_tuple(user_id, page_number, update_user_slot_address)
            # Actually write page to memory
            f.seek(page_number * PAGE_SIZE)
            f.write(page.bytearray)

            # find the next page with enough space
            other_page_number: int = get_page_with_enough_space(db_filename, updated_user_tuple_size)
            final_page_number = other_page_number

            # get page
            page: Page = create_empty_page()
            f.seek(other_page_number * PAGE_SIZE)
            page.load_bytes(bytearray(f.read(page.page_size)))

        else:
            # TODO scuffed but easy: just remove tuple and re-append
            # enough space, we can just write the user tuple to the page
            # Remove old user tuple
            page.remove_tuple(user_id, page_number, update_user_slot_address)
            final_page_number = page_number

        # write user to page
        new_offset_address: int = page.append_tuple(encoded_updated_user_tuple)

        # add page and user offset to user index
        user_index.insert_keyval(user_id, final_page_number.to_bytes(8, 'little') + new_offset_address.to_bytes(8, 'little'))
        # user_index[user_id] = final_page_number.to_bytes(8, 'little') + new_offset_address.to_bytes(8, 'little')

        # update remaining page mem index
        remaining_page_mem_index[final_page_number] -= updated_user_tuple_size + OFFSET_SIZE

        # write page to binary file
        f.seek(final_page_number * PAGE_SIZE)
        f.write(page.bytearray)

    print("userID ", user_id, " from page ", page_number, " to page ", final_page_number)



def test_code():
    # Reduce page size for easier testing
    global PAGE_SIZE
    OLD_PAGE_SIZE = PAGE_SIZE
    PAGE_SIZE = 256

    """Simple test code that was manually modified configured to check for specific bugs.
    This test code does NOT cover all cases and is mainly oriented towards small scale,
    manual verification of expected results.
    """
    print("""
#########################
# CREATE TEST DATAFRAME #
#########################
""", flush=True)
    # make a dataframe with a few users with random data with columns: id, name, email, phone, company, street, street_number, zipcode, country_dct, birthdate_ts, initialize with array
    df.loc[0] = [1, 'John Doe', 'johndoe@gmail.com', '1234567890', 'Google', '1600 Amphitheatre Parkway', 1600, 94043,
                 1, 1234567890]
    df.loc[1] = [2, 'Johnny Smith', 'johnsmith@gmail.com', '1234567890', 'Google', '1600 Amphitheatre Parkway', 1600,
                 94043, 1, 1234567890]
    df.loc[2] = [3, 'Bill Doe', 'billdoe@gmail.com', '1234567890', 'Google', '1600 Amphitheatre Parkway', 1600, 94043,
                 1, 1234567890]
    df.loc[3] = [4, 'Jane Doe', 'janedoe@gmail.com', '1234567890', 'Google', '1600 Amphitheatre Parkway', 1600, 94043,
                 1, 1234567890]

    df.loc[0] = [1, 'A', 'A@m.c', '1', 'G', 'addr', 1, 2, 1, 1234567890]
    df.loc[1] = [2, 'B', 'B@m.c', '12', 'G', 'addr', 1, 2, 1, 1234567890]
    df.loc[2] = [3, 'C', 'C@m.c', '123', 'G', 'addr', 1, 2, 1, 1234567890]
    df.loc[3] = [4, 'D', 'D@m.c', '1234', 'G', 'addr', 1, 2, 1, 1234567890]
    df.loc[4] = [5, 'E', 'E@m.c', '12345', 'G', 'addr', 1, 2, 1, 1234567890]

    for uid in range(97 + 5, 97 + 26):
        df.loc[uid] = [uid, chr(uid), chr(uid) + '@m.c', '12345', 'G', 'addr', 1, 2, 1, 1234567890]

    save_users_to_binary_var_length("test.bin", df)




    # print(user_index.get(1))
    # print(user_index.get(2))
    # print(user_index.get(3))
    # print(user_index.get(4))
    # print(int.from_bytes(user_index.get(1)[0:8], 'little'), int.from_bytes(user_index.get(1)[8:16], 'little'))
    # print(int.from_bytes(user_index.get(2)[0:8], 'little'), int.from_bytes(user_index.get(2)[8:16], 'little'))
    # print(int.from_bytes(user_index.get(3)[0:8], 'little'), int.from_bytes(user_index.get(3)[8:16], 'little'))
    # print(int.from_bytes(user_index.get(4)[0:8], 'little'), int.from_bytes(user_index.get(4)[8:16], 'little'))
    # print("\n")

    print("""
#############################
# DISPLAY INITIAL DATAFRAME #
#############################
""", flush=True)

    df2 = load_users_from_binary_var_length("test.bin")
    display(df2)

    print("""
##############
# READ USERS #
##############
""", flush=True)

    #Read 4 random users
    print("------ BEFORE DELETE ------")
    random_ids = [1, 2, 3, 4, 5] + list(range(97 + 5, 97 + 26))
    random_users = []
    for id in random_ids:
        user_i = read_var_length_user("test.bin", id)
        random_users.append(user_i)
    df_sample = pd.DataFrame(random_users, columns=new_user_columns)
    display(df_sample)

    print("""
################
# DELETE USERS #
################
""", flush=True)

    delete_var_length_user("test.bin", 3)
    delete_var_length_user("test.bin", 1)
    delete_var_length_user("test.bin", 5)
    delete_var_length_user("test.bin", 4)


    #Read 4 random users
    print("------ AFTER DELETE ------")
    random_ids = [1, 2, 3, 4, 5]
    random_users = []
    for id in random_ids:
        user_i = read_var_length_user("test.bin", id)
        if user_i is None:
            continue
        random_users.append(user_i)
    df_sample2 = load_users_from_binary_var_length("test.bin")
    display(df_sample2)

    print("""
################
# CREATE USERS #
################
""", flush=True)

    # create new user
    new_user = [1000, 'A', 'A@m.c', '1', 'G', 'addr', 1, 2, 1, 1234567890]
    new_user2 = [2000, 'G', 'G@m.c', '1', 'G', 'addr', 1, 2, 1, 1234567890]
    create_var_length_user("test.bin", new_user)
    create_var_length_user("test.bin", new_user2)
    for uid in range(10000, 10000 * 2):
        new_userx = [uid, str(uid), 'G@m.c', '1', 'G', 'addr', 1, 2, 1, 1234567890]
        create_var_length_user("test.bin", new_userx)
    df2 = load_users_from_binary_var_length("test.bin")
    display(df2)

    print("""
################
# UPDATE USERS #
################
""", flush=True)

    # update user
    for new_user_id in range(105, 115):
        new_user = [new_user_id, str(new_user_id), str(new_user_id) * 5 + '@m.c', '3', 'H', 'addr', 1, 2, 1, 1333566890]
        update_var_length_user("test.bin", new_user_id, new_user)

    df2 = load_users_from_binary_var_length("test.bin")
    display(df2)

    # Reinstate page size
    PAGE_SIZE = OLD_PAGE_SIZE


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test_code()

