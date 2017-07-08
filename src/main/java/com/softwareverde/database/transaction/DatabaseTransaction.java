package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseException;

public interface DatabaseTransaction<T> {
    void execute(final DatabaseRunnable<T> databaseRunnable) throws DatabaseException;
}
