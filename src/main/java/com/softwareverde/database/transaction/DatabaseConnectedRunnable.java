package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

public interface DatabaseConnectedRunnable<T> {
    public void run(final DatabaseConnection<T> databaseConnection) throws DatabaseException;
}
