package com.softwareverde.database.transaction;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcDatabaseTransaction implements DatabaseTransaction<Connection> {

    private final DatabaseConnectionProvider<Connection> _databaseConnectionProvider;

    public JdbcDatabaseTransaction(DatabaseConnectionProvider<Connection> databaseConnectionProvider) {
        _databaseConnectionProvider = databaseConnectionProvider;
    }

    @Override
    public void execute(final DatabaseConnectedRunnable<Connection> databaseConnectedRunnable) throws DatabaseException {
        try (final DatabaseConnection<Connection> databaseConnection = _databaseConnectionProvider.getNewDatabaseConnection()) {
            Connection connection = null;
            try {
                connection = databaseConnection.getRawConnection();
                connection.setAutoCommit(false);

                databaseConnectedRunnable.run(databaseConnection);

                connection.commit();
            } catch (Exception e) {
                quietRollback(connection);
                throw new DatabaseException("Unable to complete action.", e);
            }
        }
    }

    private void quietRollback(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                // drop exception
            }
        }
    }
}
