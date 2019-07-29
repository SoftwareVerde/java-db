package com.softwareverde.database.memory.mysql;

import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.query.Query;
import com.softwareverde.database.row.Row;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;

import java.sql.*;

public class MysqlMemoryDatabaseConnection implements DatabaseConnection<Connection> {
    private final Connection _connection;

    private TypedParameter[] _getTypedParametersAsArray(final List<TypedParameter> typedParameters) {
        final TypedParameter[] typedParameterArray = new TypedParameter[typedParameters.getSize()];
        for (int i = 0; i < typedParameterArray.length; ++i) {
            typedParameterArray[i] = typedParameters.get(i);
        }
        return typedParameterArray;
    }

    private TypedParameter[] _stringArrayToTypedParameters(final String[] parameters) {
        if (parameters == null) { return null; }

        final TypedParameter[] typedParameters = new TypedParameter[parameters.length];
        for (int i=0; i<parameters.length; ++i) {
            typedParameters[i] = new TypedParameter(parameters[i], ParameterType.STRING);
        }
        return typedParameters;
    }

    private List<Row> _query(final String query, final TypedParameter[] typedParameters) throws DatabaseException {
        try {
            final MutableList<Row> results = new MutableList<Row>();
            try (final PreparedStatement preparedStatement = _prepareStatement(query, typedParameters);
                 final ResultSet resultSet = preparedStatement.executeQuery() ) {

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

    private PreparedStatement _prepareStatement(final String query, final TypedParameter[] parameters) throws SQLException {
        final PreparedStatement preparedStatement = _connection.prepareStatement(query);
        if (parameters != null) {
            for (int i = 0; i < parameters.length; ++i) {
                final TypedParameter typedParameter = parameters[i];
                switch (typedParameter.type) {
                    case STRING: {
                        preparedStatement.setString(i + 1, (String) parameters[i].value);
                    } break;
                    case BYTE_ARRAY: {
                        preparedStatement.setBytes(i + 1, (byte[]) parameters[i].value);
                    } break;
                    case BOOLEAN: {
                        preparedStatement.setBoolean(i + 1, (Boolean) parameters[i].value);
                    }
                }
            }
        }
        return preparedStatement;
    }

    private void _executeSql(final String query, final TypedParameter[] parameters) throws DatabaseException {
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
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        _executeSql(query, typedParameters);
        return 0L;
    }

    @Override
    public Long executeSql(final Query query) throws DatabaseException {
        _executeSql(query.getQueryString(), _getTypedParametersAsArray(query.getParameters()));
        return 0L;
    }

    @Override
    public List<Row> query(final String query, final String[] parameters) throws DatabaseException {
        final TypedParameter[] typedParameters = _stringArrayToTypedParameters(parameters);
        return _query(query, typedParameters);
    }

    @Override
    public List<Row> query(final Query query) throws DatabaseException {
        return _query(query.getQueryString(), _getTypedParametersAsArray(query.getParameters()));
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
