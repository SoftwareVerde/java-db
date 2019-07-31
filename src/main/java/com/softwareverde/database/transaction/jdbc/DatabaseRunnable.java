package com.softwareverde.database.transaction.jdbc;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

public interface DatabaseRunnable<T> {
    void run(final DatabaseConnection<T> databaseConnection) throws DatabaseException;
}
