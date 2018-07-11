package com.softwareverde.database;

public interface DatabaseConnectionFactory<T> {
    DatabaseConnection<T> newConnection() throws DatabaseException;
}
