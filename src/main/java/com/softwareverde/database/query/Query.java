package com.softwareverde.database.query;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.database.query.parameter.TypedParameter;

import java.util.ArrayList;
import java.util.List;

public class Query {
    protected final String _query;
    protected final List<TypedParameter> _parameters = new ArrayList<TypedParameter>();

    protected void _setByteArray(final byte[] bytes) {
        _parameters.add(new TypedParameter(bytes));
    }

    public Query(final String query) {
        _query = query;
    }

    public Query setParameter(final Object value) {
        if (value == null) {
            _parameters.add(TypedParameter.NULL);
            return this;
        }

        if (value instanceof Boolean) {
            _parameters.add(new TypedParameter((Boolean) value));
        }
        else if (value instanceof Long) {
            _parameters.add(new TypedParameter((Long) value));
        }
        else if (value instanceof Integer) {
            _parameters.add(new TypedParameter((Integer) value));
        }
        else if (value instanceof Short) {
            _parameters.add(new TypedParameter((Short) value));
        }
        else if (value instanceof Double) {
            _parameters.add(new TypedParameter((Double) value));
        }
        else if (value instanceof Float) {
            _parameters.add(new TypedParameter((Float) value));
        }
        else if (value instanceof byte[]) {
            _setByteArray((byte[]) value);
        }
        else if (value instanceof ByteArray) {
            _setByteArray(((ByteArray) value).getBytes());
        }
        else if (value instanceof TypedParameter) {
            _parameters.add((TypedParameter) value);
        }
        else {
            _parameters.add(new TypedParameter(value.toString()));
        }

        return this;
    }

    public List<TypedParameter> getParameters() {
        return new ArrayList<TypedParameter>(_parameters);
    }

    public String getQueryString() {
        return _query;
    }
}
