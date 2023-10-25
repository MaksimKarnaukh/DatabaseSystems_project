# Database systems: Exercise 2

## Group members

Thomas Gueutal, Bas Tobback, Billy Vanhove en Maksim Karnaukh

## Challenge: Create your own database!

By default, we implemented all the five (sub)exercises in the jupyter notebook.


## Challenge: can you create a better database?

### Concerning the fixed-length binary file (fixed-length user tuples), we implemented the following functions:

Our first challenge to overcome, was to write fast functions to save and load the data with the binary file. 

For the save to binary file, We did this by using a lambda function on the dataframe which is quite fast, 
compared to a regular for loop. We did a small benchmark and this is roughly 6 times faster. 

For the load from binary file, we did a slightly different approach. 
We calculated the number of rows and just loaded the users one by one, which is around as fast as the save to binary file.

For the **read** and **write** of a single user, we implemented this as part of the assignment. We just read and write a single binary row, 
instead of decoding and encoding the entire file. 

On top of that, we also implemented the **delete user** and **insert user**. \
Firstly, the delete user: The way it works is by adding an extra column to the user tuple. This column is 
called 'deleted' and is 0 by default. When the delete user is called, this value is changed to 1. 
The reason we don't delete the tuple but rather mark is as deleted, is because with this approach, 
the read and write user can keep working efficiently because they calculate the offset based on 
id and if you were to remove an id, it would make this calculation invalid. That's why we save this 
entry and mark it, rather than deleting it. Doing it this way also allows us to overwrite it when a user 
is inserted. While we didn't go for this design with our insert, it could be fairly useful. 

The reason we didn't go for it, is because finding a deleted user in the system is O(n) time because sometimes, 
no users are deleted, so you loop over all n users. We don't save which entries are deleted but if we did, it could 
be a O(1) algorithm which would also minimise the amount of bytes used as storage because deleted users would 
just be overwritten. The way we insert users is by simply adding them to the  back of the file as an 
encoded row. This also takes O(1). The only downside of this, is that deleted users stay stuck in memory. 

If this were a real database, a solution to this, is to perform a row by row read and save unmarked rows 
and just overwrite the entire database with them. You don't save marked rows, so they would just be excluded when 
overwriting. We would also like to mention that if you delete a user, you can no longer read it by id (it returns None) 
and if you call load_from_binary_file to load all users, deleted users will also be excluded from the returned value.

### Concerning the variable-length binary file (variable-length user tuples), we implemented the following functions:

#### Encoding and decoding:

- encode_user_var_length(user) function: was already implemented, but we changed it to be able to also take a user tuple (list of attributes) as input.
- decode_user_var_length(byte_array) function: decodes a user of variable length.

#### Save and load to file varying-length to binary file + other functionality:

We first implemented the save and load functions for the variable-length binary file using one big file 
(the same way the rest of the notebook used one big file for saving bytes). But then we decided to switch the whole thing up.

As a reference to the lectures given in the class database systems, we decided to implement a system with pages (blocks of fixed size).
The binary file is now split up into pages of fixed size. 

Each page has a tuple counter (lets call it N, the first 2 bytes, since each user can have a maximum size of 298B and the used page size is 8192B, 16 bits should be enough), which keeps track of how many user tuples there are in the page.
After the first two bytes, we have the offsets (sometimes, we might use the word 'address' as a synonym) of the user tuples. These are N slots of 2 bytes each (which in total forms the slot array). The first slot value is the offset of the first user tuple, the second slot value is the offset of the second user tuple, etc.
They grow from the beginning of the page (after the first two bytes) to the end of the page.
The user tuples however grow from the end of the page to the beginning of the page. This way, the first slot value corresponds to the last user tuple in the page, and so on.

Since the user tuples are varying length and they are not ordered by id in the page, we keep a dictionary 
(originally it was a bplustree, but the best library we found and used didn't have a delete function) of the 
user ids (key) and the page number and the offset to the slot that keeps the user tuple address in the page (value)
(this way, for example if we need to get the address of the previous user tuple we can easily get this).

We also keep track of the free space in the page (= unused bytes). 
This way, we can easily check if there is enough space in the page to e.g. add a new user tuple.

If we want to add (create) a user and there is no space in any of the existing pages, we create a new page and add the user there. 
The same check is done when we want to update a user. 
If there is not enough space in the page for the updated user tuple, we create a new page and add the user there. If there is space, we update the user in the same page.

For most of this, a Page class was made that implements the functionality described.

- save_users_to_binary_var_length(filename, df) function: saves variable-length user tuples to a binary file.
- load_users_from_binary_var_length(filename) function: loads variable-length user tuples from a binary file. 

- read_var_length_user(db_filename: str, user_id: int) function: reads a user tuple with the given user id from the binary file.
- get_page_with_enough_space(db_filename: str, user_size: int) function: gets a page with enough space for a user tuple of the given size.
- create_var_length_user(db_filename: str, user_tuple) function: creates a user tuple in the binary file.
- delete_var_length_user(db_filename: str, user_id) function: deletes a user tuple with the given user id from the binary file.
- update_var_length_user(db_filename: str, user_id, updated_user_tuple) function: updates a user tuple with the given user id in the binary file.

If you want to run some random test code to check our functionality correctness, you can run the test_code() function.

