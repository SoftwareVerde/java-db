package com.softwareverde.database.query;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;

public class Query {
    protected final String _query;
    protected final MutableList<TypedParameter> _parameters = new MutableList<TypedParameter>();

    protected void _setBoolean(final boolean booleanValue) {
        _parameters.add(new TypedParameter(booleanValue, ParameterType.BOOLEAN));
    }

    protected void _setByteArray(final byte[] bytes) {
        _parameters.add(new TypedParameter(bytes, ParameterType.BYTE_ARRAY));
    }

    public Query(final String query) {
        _query = query;
    }

    public Query setParameter(final Object value) {
        if (value == null) {
            _parameters.add(new TypedParameter(null, ParameterType.STRING));
        }
        else {
            if (value instanceof Boolean) {
                _setBoolean((Boolean) value);
            }
            else if (value instanceof byte[]) {
                _setByteArray((byte[]) value);
            }
            else if (value instanceof ByteArray) {
                _setByteArray(((ByteArray) value).getBytes());
            }
            else {
                _parameters.add(new TypedParameter(value.toString(), ParameterType.STRING));
            }
        }

        return this;
    }

    public Query setParameter(final boolean value) {
        _setBoolean(value);
        return this;
    }

    public Query setParameter(final byte[] bytes) {
        _setByteArray(bytes);
        return this;
    }

    public List<TypedParameter> getParameters() {
        return _parameters;
    }

    public String getQueryString() {
        return _query;
    }
}
