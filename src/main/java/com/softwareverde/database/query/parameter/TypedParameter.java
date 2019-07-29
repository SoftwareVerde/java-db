package com.softwareverde.database.query.parameter;

import com.softwareverde.constable.bytearray.ByteArray;

public class TypedParameter {
    public static final TypedParameter NULL = new TypedParameter();

    protected static final Long TRUE = 1L;
    protected static final Long FALSE = 0L;

    public final ParameterType type;
    public final Object value;

    protected TypedParameter() {
        this.type = ParameterType.STRING;
        this.value = null;
    }

    public TypedParameter(final Boolean value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value ? TRUE : FALSE);
    }

    public TypedParameter(final String value) {
        this.type = ParameterType.STRING;
        this.value = value;
    }

    public TypedParameter(final Long value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = value;
    }

    public TypedParameter(final Integer value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value != null ? value.longValue() : null);
    }

    public TypedParameter(final Short value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value != null ? value.longValue() : null);
    }

    public TypedParameter(final Double value) {
        this.type = ParameterType.FLOATING_POINT_NUMBER;
        this.value = value;
    }

    public TypedParameter(final Float value) {
        this.type = ParameterType.FLOATING_POINT_NUMBER;
        this.value = (value != null ? value.doubleValue() : null);
    }

    public TypedParameter(final byte[] value) {
        this.type = ParameterType.BYTE_ARRAY;
        this.value = value;
    }

    public TypedParameter(final ByteArray value) {
        this.type = ParameterType.BYTE_ARRAY;
        this.value = (value != null ? value.getBytes() : null);
    }
}
