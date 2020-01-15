package com.softwareverde.database.jdbc;

import com.softwareverde.database.Database;

import java.sql.Connection;

/**
 * Convenience interface to avoid specifying the generic type everywhere when using JDBC.
 */
public interface JdbcDatabase extends Database<Connection>, JdbcDatabaseConnectionFactory { }
