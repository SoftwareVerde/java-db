package com.softwareverde.database;

public interface Database<T> {
    DatabaseConnection<T> newConnection() throws DatabaseException;
}
