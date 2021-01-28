package com.softwareverde.database.jdbc;

import com.softwareverde.database.properties.DatabaseProperties;

import java.util.Properties;

public interface JdbcDatabaseProperties extends DatabaseProperties {
    Properties getConnectionProperties();
}
