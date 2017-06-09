# Java Database Wrapper

Java wrapper for database various interfaces.

## Version 2.0 Release

    Database            - stripped down to be more akin to a connection generator.
    DatabaseException   - standardized exception thrown by library.
    DatabaseConnection  - replaces most of the old functionality of Database.
                            Exceptions are no longer swallowed.
    Query               - query-builder helper class created.
    Row                 - Extracted from Database inner-class.


## Child Packages
* java-db-mysql
* java-db-mssql
* java-db-android

