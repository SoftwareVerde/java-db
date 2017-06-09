package com.softwareverde.database;

import com.softwareverde.util.Util;

import java.util.ArrayList;
import java.util.List;

public class Query {
    protected final String _query;
    protected final List<String> _parameters = new ArrayList<String>();

    public Query(final String query) {
        _query = query;
    }

    public Query setParameter(final Object value) {
        if (value == null) {
            _parameters.add(null);
        }
        else {
            _parameters.add(value.toString());
        }

        return this;
    }

    public List<String> getParameters() {
        return Util.copyList(_parameters);
    }

    public String getQueryString() {
        return _query;
    }
}
