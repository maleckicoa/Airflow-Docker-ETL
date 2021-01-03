set role postgres; -- only works as admin of the database

set schema 'staging';


-- phonebook

CREATE TABLE phonebook(
  phone VARCHAR(32),
  firstname VARCHAR(32),
  lastname VARCHAR(32),
  address VARCHAR(64)

);

CREATE INDEX
   ON phonebook (phone);
