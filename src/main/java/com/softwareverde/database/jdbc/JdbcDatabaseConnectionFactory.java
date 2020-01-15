package com.softwareverde.database.jdbc;

import com.softwareverde.database.DatabaseConnectionFactory;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;

public interface JdbcDatabaseConnectionFactory extends DatabaseConnectionFactory<Connection> {
    @Override
    JdbcDatabaseConnection newConnection() throws DatabaseException;
}
