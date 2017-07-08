package com.softwareverde.database.memory.mysql;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MysqlMemoryDatabaseConnection implements DatabaseConnection<Connection> {
    private final Connection _connection;

    private PreparedStatement _prepareStatement(final String query, final String[] parameters) throws SQLException {
        final PreparedStatement preparedStatement = _connection.prepareStatement(query);
        if (parameters != null) {
            for (int i = 0; i < parameters.length; ++i) {
                preparedStatement.setString(i+1, parameters[i]);
            }
        }
        return preparedStatement;
    }

    private void _executeSql(final String query, final String[] parameters) throws DatabaseException {
        try (final PreparedStatement preparedStatement = _prepareStatement(query, parameters)) {
            preparedStatement.execute();
        }
        catch (final SQLException exception) {
            throw new DatabaseException(exception);
        }
    }


    public MysqlMemoryDatabaseConnection(final Connection connection) {
        _connection = connection;
    }

    @Override
    public void executeDdl(final String query) throws DatabaseException {
        try {
            final Statement statement = _connection.createStatement();
            statement.execute(query);
            statement.close();
        }
        catch (SQLException sqlException) {
            throw new DatabaseException(sqlException);
        }
    }

    @Override
    public void executeDdl(final Query query) throws DatabaseException {
        this.executeDdl(query.getQueryString());
    }

    @Override
    public Long executeSql(final String query, final String[] parameters) throws DatabaseException {
        _executeSql(query, parameters);
        return 0L;
    }

    @Override
    public Long executeSql(final Query query) throws DatabaseException {
        return this.executeSql(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        try {
            final List<Row> results = new ArrayList<Row>();
            try (
                    final PreparedStatement preparedStatement = _prepareStatement(query, parameters);
                    final ResultSet resultSet = preparedStatement.executeQuery()
                ) {

                if (resultSet.first()) {
                    do {
                        results.add(MysqlMemoryRow.fromResultSet(resultSet));
                    } while (resultSet.next());
                }
            }
            return results;
        }

        catch (final SQLException exception) {
            throw new DatabaseException("Error executing query.", exception);
        }
    }

    @Override
    public List<Row> query(final Query query) throws DatabaseException {
        return this.query(query.getQueryString(), query.getParameters().toArray(new String[0]));
    }

    @Override
    public void close() throws DatabaseException {
        try {
            _connection.close();
        }
        catch (final SQLException sqlException) {
            throw new DatabaseException(sqlException);
        }
    }

    @Override
    public Connection getRawConnection() {
        return _connection;
    }
}
