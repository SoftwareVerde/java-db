package com.softwareverde.database.transaction;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcDatabaseTransaction implements DatabaseTransaction<Connection> {
    protected final Database<Connection> _database;

    public JdbcDatabaseTransaction(final Database<Connection> database) {
        _database = database;
    }

    @Override
    public void execute(final DatabaseRunnable<Connection> databaseConnectedRunnable) throws DatabaseException {
        try (final DatabaseConnection<Connection> databaseConnection = _database.newConnection()) {
            final Connection connection = databaseConnection.getRawConnection(); // Should be closed for us by DatabaseConnection.close()

            try {
                connection.setAutoCommit(false);
                databaseConnectedRunnable.run(databaseConnection);
                connection.commit();
            }
            catch (final SQLException sqlException) {
                connection.rollback();
                throw sqlException;
            }

        }
        catch (final Exception exception) {
            throw new DatabaseException("Unable to complete query.", exception);
        }
    }
}