package com.softwareverde.database.query;

import com.softwareverde.database.query.parameter.InClauseParameter;
import com.softwareverde.database.query.parameter.TypedParameter;

public interface ValueExtractor<T> {
    ValueExtractor<Boolean> BOOLEAN = new ValueExtractor<Boolean>() {
        @Override
        public InClauseParameter extractValues(final Boolean value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<String> STRING = new ValueExtractor<String>() {
        @Override
        public InClauseParameter extractValues(final String value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<Integer> INTEGER = new ValueExtractor<Integer>() {
        @Override
        public InClauseParameter extractValues(final Integer value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<Long> LONG = new ValueExtractor<Long>() {
        @Override
        public InClauseParameter extractValues(final Long value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<Short> SHORT = new ValueExtractor<Short>() {
        @Override
        public InClauseParameter extractValues(final Short value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<Float> FLOAT = new ValueExtractor<Float>() {
        @Override
        public InClauseParameter extractValues(final Float value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<Double> DOUBLE = new ValueExtractor<Double>() {
        @Override
        public InClauseParameter extractValues(final Double value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    ValueExtractor<byte[]> BYTE_ARRAY = new ValueExtractor<byte[]>() {
        @Override
        public InClauseParameter extractValues(final byte[] value) {
            final TypedParameter typedParameter = (value != null ? new TypedParameter(value) : TypedParameter.NULL);
            return new InClauseParameter(typedParameter);
        }
    };

    InClauseParameter extractValues(T value);
}
