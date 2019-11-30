package com.softwareverde.database.query.parameter;

public interface ParameterFactory {
    TypedParameter fromObject(Object object);
}
