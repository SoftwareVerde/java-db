package com.softwareverde.database.row;

import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;
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

    protected final List<String> _orderedColumnNames = new ArrayList<String>();
    protected final Map<String, TypedParameter> _columnValues = new HashMap<String, TypedParameter>();

    protected JdbcRow() { }

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
        final String stringValue = _getString(columnName);
        return Util.parseInt(stringValue);
    }

    @Override
    public Long getLong(final String columnName) {
        final String stringValue = _getString(columnName);
        return Util.parseLong(stringValue);
    }

    @Override
    public Float getFloat(final String columnName) {
        final String stringValue = _getString(columnName);
        return Util.parseFloat(stringValue);
    }

    @Override
    public Double getDouble(final String columnName) {
        final String stringValue = _getString(columnName);
        return Util.parseDouble(stringValue);
    }

    @Override
    public Boolean getBoolean(final String columnName) {
        final TypedParameter value = _getValue(columnName);

        if (value.type != ParameterType.BOOLEAN) {
            final String stringValue = _getString(columnName);
            return Util.parseBool(stringValue);
        }

        return (boolean) value.value;
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
