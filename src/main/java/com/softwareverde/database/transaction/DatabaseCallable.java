package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

public interface DatabaseCallable<ReturnType, ConnectionType> {
    ReturnType call(final DatabaseConnection<ConnectionType> databaseConnection) throws DatabaseException;
}
