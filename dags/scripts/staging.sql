/*
*** PostgreSQL script for staging the analytics database ***

This script is executed when the load of a database dump is completed. It should contain all statements that are required to condition data for later analysis.
*/


set schema 'staging';


/* populate tables */

-- phonebook
TRUNCATE TABLE phonebook;

INSERT INTO phonebook (
    select
    phone as phone,
    firstname as firstname,
    lastname as lastname,
    address as address
    from
    prod.phonebook p
);
