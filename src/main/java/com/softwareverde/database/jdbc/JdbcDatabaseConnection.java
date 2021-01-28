package com.softwareverde.database.jdbc;

import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.jdbc.row.JdbcRowFactory;
import com.softwareverde.database.query.Query;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.database.row.Row;
import com.softwareverde.database.row.RowFactory;
import com.softwareverde.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcDatabaseConnection implements DatabaseConnection<Connection>, AutoCloseable {
    protected static final Long INVALID_ID = -1L;
    protected static final Integer INVALID_ROW_COUNT = -1;

    protected final RowFactory _rowFactory;
    protected final Connection _connection;
    protected Long _lastInsertId = INVALID_ID;
    protected Integer _lastRowAffectedCount = INVALID_ROW_COUNT;

    protected TypedParameter[] _stringArrayToTypedParameters(final String[] parameters) {
        if (parameters == null) { return new TypedParameter[0]; }

        final TypedParameter[] typedParameters = new TypedParameter[parameters.length];
        for (int i = 0; i < parameters.length; ++i) {
            typedParameters[i] = new TypedParameter(parameters[i]);
        }
        return typedParameters;
    }

    protected Long _extractInsertId(final PreparedStatement preparedStatement) throws SQLException {
        try (final ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
            final Long insertId = (resultSet.next() ? resultSet.getLong(1) : null);
            return Util.coalesce(insertId, INVALID_ID);
        }
    }

    protected PreparedStatement _prepareStatement(final String query, final TypedParameter[] parameters) throws SQLException {
        final PreparedStatement preparedStatement = _connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        if (parameters != null) {
            for (int i = 0; i < parameters.length; ++i) {
                final int parameterIndex = (i + 1);
                final TypedParameter typedParameter = parameters[i];
                switch (typedParameter.type) {
                    case STRING: {
                        preparedStatement.setString(parameterIndex, (String) parameters[i].value);
                    } break;
                    case BYTE_ARRAY: {
                        preparedStatement.setBytes(parameterIndex, (byte[]) parameters[i].value);
                    } break;
                    case WHOLE_NUMBER: {
                        preparedStatement.setLong(parameterIndex, (Long) parameters[i].value);
                    } break;
                    case FLOATING_POINT_NUMBER: {
                        preparedStatement.setDouble(parameterIndex, (Double) parameters[i].value);
                    } break;
                }
            }
        }
        return preparedStatement;
    }

    protected void _executeAsPreparedStatement(final String query, final TypedParameter[] typedParameters) throws SQLException {
        try (final PreparedStatement preparedStatement = _prepareStatement(query, typedParameters)) {
            preparedStatement.execute();
            _lastInsertId = _extractInsertId(preparedStatement);
            _lastRowAffectedCount = preparedStatement.getUpdateCount();
        }
        catch (final SQLException exception) {
            _lastInsertId = INVALID_ID;
            _lastRowAffectedCount = INVALID_ROW_COUNT;
            throw exception;
        }
    }

    protected Long _executeSql(final String query, final TypedParameter[] parameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute SQL statement while disconnected.");
            }

            _executeAsPreparedStatement(query, parameters);
            return _lastInsertId;
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing SQL statement.", exception);
        }
    }

    protected List<Row> _query(final String query, final TypedParameter[] typedParameters) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute query while disconnected.");
            }

            final List<Row> results = new ArrayList<Row>();
            try (final PreparedStatement preparedStatement = _prepareStatement(query, typedParameters);
                 final ResultSet resultSet = preparedStatement.executeQuery() ) {

                if (resultSet.first()) {
                    do {
                        results.add(_rowFactory.fromResultSet(resultSet));
                    } while (resultSet.next());
                }
            }
            return results;
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing query.", exception);
        }
    }

    protected JdbcDatabaseConnection(final Connection connection, final RowFactory rowFactory) {
        _rowFactory = rowFactory;
        _connection = connection;
    }

    public JdbcDatabaseConnection(final Connection connection) {
        _rowFactory = new JdbcRowFactory();
        _connection = connection;
    }

    public Integer getRowsAffectedCount() {
        return _lastRowAffectedCount;
    }

    @Override
    public synchronized void executeDdl(final String query) throws DatabaseException {
        try {
            if (_connection.isClosed()) {
                throw new DatabaseException("Attempted to execute DDL statement while disconnected.");
            }

            try (final Statement statement = _connection.createStatement()) {
                statement.execute(query);
            }
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Error executing DDL statement.", exception);
        }
    }

    @Override
    public synchronized void executeDdl(final Query query) throws DatabaseException {
        this.executeDdl(query.getQueryString());
    }

    @Override
    public synchronized Long executeSql(final String query, final String[] parameters) throws DatabaseException {
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        return _executeSql(query, typedParameters);
    }

    @Override
    public synchronized Long executeSql(final Query query) throws DatabaseException {
        final String queryString = query.getQueryString();
        final List<TypedParameter> parameters = query.getParameters();
        return _executeSql(queryString, parameters.toArray(new TypedParameter[0]));
    }

    @Override
    public synchronized List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        return _query(query, typedParameters);
    }

    @Override
    public synchronized List<Row> query(final Query query) throws DatabaseException {
        final String queryString = query.getQueryString();
        final List<TypedParameter> parameters = query.getParameters();
        return _query(queryString, parameters.toArray(new TypedParameter[0]));
    }

    @Override
    public Connection getRawConnection() {
        return _connection;
    }

    @Override
    public void close() throws DatabaseException {
        try {
            _connection.close();
        }
        catch (final SQLException exception) {
            throw new DatabaseException("Unable to close database connection.", exception);
        }
    }
}
