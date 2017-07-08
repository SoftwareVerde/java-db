package com.softwareverde.database.transaction;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcDatabaseTransaction implements DatabaseTransaction<Connection> {

    protected final Database<Connection> _database;

    protected void _quietRollback(final Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            }
            catch (final SQLException exception) { }
        }
    }

    public JdbcDatabaseTransaction(final Database<Connection> database) {
        _database = database;
    }

    @Override
    public void execute(final DatabaseRunnable<Connection> databaseRunnable) throws DatabaseException {
        try (final DatabaseConnection<Connection> databaseConnection = _database.newConnection()) {
            Connection connection = null;
            try {
                connection = databaseConnection.getRawConnection();
                connection.setAutoCommit(false);

                databaseRunnable.run(databaseConnection);

                connection.commit();
            }
            catch (final Exception exception) {
                _quietRollback(connection);
                throw new DatabaseException("Unable to complete action.", exception);
            }
        }
    }
}
