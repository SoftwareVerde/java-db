package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseConnectionFactory;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;

public class JdbcDatabaseTransaction implements DatabaseTransaction<Connection> {
    protected final DatabaseConnectionFactory<Connection> _databaseConnectionFactory;

    public JdbcDatabaseTransaction(final DatabaseConnectionFactory<Connection> databaseConnectionFactory) {
        _databaseConnectionFactory = databaseConnectionFactory;
    }

    @Override
    public void execute(final DatabaseRunnable<Connection> databaseConnectedRunnable) throws DatabaseException {
        try (final DatabaseConnection<Connection> databaseConnection = _databaseConnectionFactory.newConnection()) {
            final Connection connection = databaseConnection.getRawConnection(); // Should be closed for us by DatabaseConnection.close()

            try {
                connection.setAutoCommit(false);
                databaseConnectedRunnable.run(databaseConnection);
                connection.commit();
            }
            catch (final Exception exception) {
                connection.rollback();
                throw exception;
            }

        }
        catch (final Exception exception) {
            throw new DatabaseException("Unable to complete query.", exception);
        }
    }
}