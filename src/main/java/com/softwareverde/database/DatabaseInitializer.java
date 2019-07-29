package com.softwareverde.database;

import com.softwareverde.database.properties.DatabaseProperties;

public interface DatabaseInitializer<T> {
    interface DatabaseUpgradeHandler<T> {
        Boolean onUpgrade(DatabaseConnection<T> maintenanceDatabaseConnection, Integer previousVersion, Integer requiredVersion);
    }

    /**
     * Creates the schema if it does not exist and a maintenance user to use instead of root.
     *  The maintenance username is [schema]_maintenance; its password being the sha256 hash of the root password.
     *  Returns the maintenance credentials created by this call.
     */
    void initializeSchema(final DatabaseConnection<T> rootDatabaseConnection, final DatabaseProperties databaseProperties) throws DatabaseException;

    Integer getDatabaseVersionNumber(final DatabaseConnection<T> databaseConnection);

    void initializeDatabase(final DatabaseConnection<T> maintenanceDatabaseConnection) throws DatabaseException;
}
