package com.softwareverde.database.query;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.database.query.parameter.ParameterFactory;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.type.identifier.Identifier;

public class ParameterFactoryCore implements ParameterFactory {
    @Override
    public TypedParameter fromObject(final Object object) {
        if (object == null) { return TypedParameter.NULL; }

        if (object instanceof Boolean) {
            return new TypedParameter((Boolean) object);
        }
        else if (object instanceof Long) {
            return new TypedParameter((Long) object);
        }
        else if (object instanceof Integer) {
            return new TypedParameter((Integer) object);
        }
        else if (object instanceof Short) {
            return new TypedParameter((Short) object);
        }
        else if (object instanceof Double) {
            return new TypedParameter((Double) object);
        }
        else if (object instanceof Float) {
            return new TypedParameter((Float) object);
        }
        else if (object instanceof byte[]) {
            return new TypedParameter((byte[]) object);
        }
        else if (object instanceof ByteArray) {
            return new TypedParameter(((ByteArray) object).getBytes());
        }
        else if (object instanceof Identifier) {
            return new TypedParameter(((Identifier) object).longValue());
        }
        else if (object instanceof TypedParameter) {
            return (TypedParameter) object;
        }
        else if (object instanceof String) {
            return new TypedParameter((String) object);
        }

        // Warning is not logged to facilitate class extension.
        // final Class<?> objectClass = object.getClass();
        // Logger.warn("Unknown Object for TypedParameter Factory: " + objectClass.getName());

        return null;
    }
}
