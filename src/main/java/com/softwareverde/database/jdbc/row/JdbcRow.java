package com.softwareverde.database.jdbc.row;

import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.database.row.Row;
import com.softwareverde.util.Util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcRow implements Row {
    public static final String STRING_ENCODING = "ISO-8859-1";

    protected static final Charset CHARSET;
    static {
        Charset charset;
        try {
            charset = Charset.forName(STRING_ENCODING);
        }
        catch (final Exception exception) {
            charset = Charset.defaultCharset();
        }
        CHARSET = charset;
    }

    protected final List<String> _orderedColumnNames;
    protected final Map<String, TypedParameter> _columnValues;

    protected JdbcRow() {
        _orderedColumnNames = new ArrayList<String>();
        _columnValues = new HashMap<String, TypedParameter>();
    }

    /**
     * Consumes the internal values of the provided jdbcRow.
     */
    protected JdbcRow(final JdbcRow jdbcRow) {
        _orderedColumnNames = jdbcRow._orderedColumnNames;
        _columnValues = jdbcRow._columnValues;
    }

    protected TypedParameter _getValue(final String columnName) throws IllegalArgumentException {
        final String lowerCaseColumnName = columnName.toLowerCase();
        if (! _columnValues.containsKey(lowerCaseColumnName)) {
            throw new IllegalArgumentException("Row does not contain column: "+ columnName);
        }

        return _columnValues.get(lowerCaseColumnName);
    }

    protected String _getString(final String columnName) {
        final TypedParameter value = _getValue(columnName);

        if (value.type == ParameterType.BYTE_ARRAY) {
            return new String((byte[]) value.value, CHARSET);
        }
        else if (value.type != ParameterType.STRING) {
            return ( (value.value != null) ? value.value.toString() : null);
        }

        return (String) value.value;
    }

    protected Long _getWholeNumber(final String columnName) {
        final TypedParameter value = _getValue(columnName);
        if (value.value == null) { return null; }

        if (value.type == ParameterType.WHOLE_NUMBER) {
            return ((Long) value.value);
        }
        else if (value.type == ParameterType.FLOATING_POINT_NUMBER) {
            return ((Double) value.value).longValue();
        }
        else {
            final String stringValue = _getString(columnName);
            return Util.parseLong(stringValue);
        }
    }

    protected Double _getFloatingPointNumber(final String columnName) {
        final TypedParameter value = _getValue(columnName);
        if (value.value == null) { return null; }

        if (value.type == ParameterType.FLOATING_POINT_NUMBER) {
            return ((Double) value.value);
        }
        else if (value.type == ParameterType.WHOLE_NUMBER) {
            return ((Long) value.value).doubleValue();
        }
        else {
            final String stringValue = _getString(columnName);
            return Util.parseDouble(stringValue);
        }
    }

    /**
     * Returns the column names in order.
     */
    @Override
    public List<String> getColumnNames() {
        return new ArrayList<String>(_orderedColumnNames);
    }

    @Override
    public String getString(final String columnName) {
        return _getString(columnName);
    }

    @Override
    public Integer getInteger(final String columnName) {
        final Long value = _getWholeNumber(columnName);
        if (value == null) { return null; }

        return value.intValue();
    }

    @Override
    public Long getLong(final String columnName) {
        return _getWholeNumber(columnName);
    }

    @Override
    public Float getFloat(final String columnName) {
        final Double value = _getFloatingPointNumber(columnName);
        if (value == null) { return null; }

        return value.floatValue();
    }

    @Override
    public Double getDouble(final String columnName) {
        return _getFloatingPointNumber(columnName);
    }

    @Override
    public Boolean getBoolean(final String columnName) {
        final Long value = _getWholeNumber(columnName);
        if (value == null) { return null; }

        return (value > 0L);
    }

    @Override
    public byte[] getBytes(final String columnName) {
        final TypedParameter value = _getValue(columnName);

        if (value.type != ParameterType.BYTE_ARRAY) {
            final String stringValue = _getString(columnName);
            if (stringValue == null) { return null; }

            return stringValue.getBytes(CHARSET);
        }

        return (byte[]) value.value;
    }
}
