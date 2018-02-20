package com.softwareverde.database.query.parameter;

public class TypedParameter {
    public final ParameterType type;
    public final Object value;

    public TypedParameter(final Object value, final ParameterType type) {
        this.type = type;
        this.value = value;
    }
}
