package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseException;

public interface DatabaseTransaction<T> {
    public void execute(final DatabaseConnectedRunnable<T> databaseConnectedRunnable) throws DatabaseException;
}
