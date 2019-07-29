package com.softwareverde.database.row;

import com.softwareverde.database.DatabaseException;
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

    // NOTE: Types.ROWID is intentionally treated as a String...
    protected static Boolean isWholeNumberType(final Integer sqlDataType) {
        switch (sqlDataType) {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.BOOLEAN:
            case Types.BIT: {
                return true;
            }
            default: { return false; }
        }
    }

    // NOTE: Types.NUMERIC and Types.DECIMAL are fixed-precision types, and are therefore stored internally as strings in order to maintain the precision...
    protected static Boolean isFloatingPointNumberType(final Integer sqlDataType) {
        switch (sqlDataType) {
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL: {
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

                final String columnName = metaData.getColumnLabel(columnIndex).toLowerCase();
                final TypedParameter typedValue;
                {
                    if (JdbcRowFactory.isWholeNumberType(sqlDataType)) {
                        final Long longValue = resultSet.getLong(columnIndex);
                        typedValue = new TypedParameter(longValue);
                    }
                    else if (JdbcRowFactory.isBinaryType(sqlDataType)) {
                        final byte[] bytes = resultSet.getBytes(columnIndex);
                        typedValue = new TypedParameter(bytes);
                    }
                    else if (JdbcRowFactory.isFloatingPointNumberType(sqlDataType)) {
                        final Double doubleValue = resultSet.getDouble(columnIndex);
                        typedValue = new TypedParameter(doubleValue);
                    }
                    else {
                        final String stringValue = resultSet.getString(columnIndex);
                        typedValue = new TypedParameter(stringValue);
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
