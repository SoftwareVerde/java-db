package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.jdbc.transaction.DatabaseCallable;
import com.softwareverde.database.jdbc.transaction.DatabaseRunnable;

public interface DatabaseTransaction<ConnectionType> {
    void execute(final DatabaseRunnable<ConnectionType> databaseRunnable) throws DatabaseException;
    <ReturnType> ReturnType call(final DatabaseCallable<ReturnType, ConnectionType> databaseCallable) throws DatabaseException;
}
