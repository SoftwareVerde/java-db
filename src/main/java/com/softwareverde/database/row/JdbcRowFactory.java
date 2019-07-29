package com.softwareverde.database.row;

import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class JdbcRowFactory implements RowFactory {

    protected static Boolean isBinaryType(final Integer sqlDataType) {
        switch (sqlDataType) {
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                return true;
            }
            default: { return false; }
        }
    }

    public JdbcRow fromResultSet(final ResultSet resultSet) throws DatabaseException {
        final JdbcRow jdbcRow = new JdbcRow();

        try {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); ++i) {
                final int columnIndex = (i + 1);
                final Integer sqlDataType = metaData.getColumnType(columnIndex);
                final Boolean isBinaryType = JdbcRowFactory.isBinaryType(sqlDataType);

                final String columnName = metaData.getColumnLabel(columnIndex).toLowerCase();
                final TypedParameter typedValue;
                {
                    if (isBinaryType) {
                        final byte[] bytes = resultSet.getBytes(columnIndex);
                        typedValue = new TypedParameter(bytes, ParameterType.BYTE_ARRAY);
                    }
                    else {
                        final String stringValue = resultSet.getString(columnIndex);
                        typedValue = new TypedParameter(stringValue, ParameterType.STRING);
                    }
                }

                jdbcRow._orderedColumnNames.add(columnName);
                jdbcRow._columnValues.put(columnName, typedValue);
            }
        }
        catch (final SQLException exception) {
            throw new DatabaseException(exception);
        }

        return jdbcRow;
    }
}
