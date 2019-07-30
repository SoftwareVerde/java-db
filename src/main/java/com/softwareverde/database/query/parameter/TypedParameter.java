package com.softwareverde.database.query.parameter;

public class TypedParameter {
    /**
     * Provided only for efficiency so a new TypedParameter doesn't have to be created for a null value.
     *  Typically, the types for a null value have little meaning, so only one instance is provided.
     */
    public static final TypedParameter NULL = new TypedParameter();

    protected static final Long TRUE = 1L;
    protected static final Long FALSE = 0L;

    public final ParameterType type;
    public final Object value;

    protected TypedParameter() {
        this.type = ParameterType.STRING;
        this.value = null;
    }

    /**
     * Creates a Boolean TypedParameter.  The value may be null.
     */
    public TypedParameter(final Boolean value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value ? TRUE : FALSE);
    }

    /**
     * Creates a String TypedParameter.  The value may be null.
     */
    public TypedParameter(final String value) {
        this.type = ParameterType.STRING;
        this.value = value;
    }

    /**
     * Creates a Long TypedParameter.  The value may be null.
     */
    public TypedParameter(final Long value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = value;
    }

    /**
     * Creates a Integer TypedParameter.  The value may be null.
     */
    public TypedParameter(final Integer value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value != null ? value.longValue() : null);
    }

    /**
     * Creates a Short TypedParameter.  The value may be null.
     */
    public TypedParameter(final Short value) {
        this.type = ParameterType.WHOLE_NUMBER;
        this.value = (value != null ? value.longValue() : null);
    }

    /**
     * Creates a Double TypedParameter.  The value may be null.
     */
    public TypedParameter(final Double value) {
        this.type = ParameterType.FLOATING_POINT_NUMBER;
        this.value = value;
    }

    /**
     * Creates a Float TypedParameter.  The value may be null.
     */
    public TypedParameter(final Float value) {
        this.type = ParameterType.FLOATING_POINT_NUMBER;
        this.value = (value != null ? value.doubleValue() : null);
    }

    /**
     * Creates a byte[] TypedParameter.  The value may be null.
     */
    public TypedParameter(final byte[] value) {
        this.type = ParameterType.BYTE_ARRAY;
        this.value = value;
    }
}
