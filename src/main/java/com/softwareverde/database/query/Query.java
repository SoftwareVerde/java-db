package com.softwareverde.database.query;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.type.identifier.Identifier;

import java.util.ArrayList;
import java.util.List;

public class Query {
    protected final String _query;
    protected final List<TypedParameter> _parameters = new ArrayList<TypedParameter>();

    public Query(final String query) {
        _query = query;
    }

    public Query setParameter(final Boolean value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Long value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Integer value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Short value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Double value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Float value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final byte[] value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final ByteArray value) {
        _parameters.add((value != null) ? new TypedParameter(value.getBytes()) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final Identifier value) {
        _parameters.add((value != null) ? new TypedParameter(value.longValue()) : TypedParameter.NULL);
        return this;
    }

    public Query setParameter(final TypedParameter value) {
        _parameters.add(value);
        return this;
    }

    public Query setParameter(final String value) {
        _parameters.add((value != null) ? new TypedParameter(value) : TypedParameter.NULL);
        return this;
    }

    public List<TypedParameter> getParameters() {
        return new ArrayList<TypedParameter>(_parameters);
    }

    public String getQueryString() {
        return _query;
    }
}
