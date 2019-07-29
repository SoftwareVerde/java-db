package com.softwareverde.database;

import com.softwareverde.database.properties.DatabaseCredentials;
import com.softwareverde.database.properties.DatabaseProperties;

public interface DatabaseInitializer<T> {
    interface DatabaseUpgradeHandler<T> {
        Boolean onUpgrade(DatabaseConnection<T> maintenanceDatabaseConnection, Integer previousVersion, Integer requiredVersion);
    }

    /**
     * Creates the schema if it does not exist and a maintenance user to use instead of root.
     */
    void initializeSchema(DatabaseConnection<T> rootDatabaseConnection, DatabaseProperties databaseProperties) throws DatabaseException;

    DatabaseCredentials getMaintenanceCredentials(DatabaseProperties databaseProperties);

    Integer getDatabaseVersionNumber(DatabaseConnection<T> databaseConnection);

    void initializeDatabase(DatabaseConnection<T> maintenanceDatabaseConnection) throws DatabaseException;
}
