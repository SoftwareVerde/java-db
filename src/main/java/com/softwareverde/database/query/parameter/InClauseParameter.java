package com.softwareverde.database.query.parameter;

import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableListBuilder;

public class InClauseParameter {
    public static final InClauseParameter NULL = new InClauseParameter();

    public enum Type {
        NULL, SINGLE, TUPLE
    }

    protected final Type _type;
    protected final List<TypedParameter> _parameters;

    public InClauseParameter(final TypedParameter... typedParameters) {
        if ( (typedParameters == null) || (typedParameters.length == 0) ) {
            _type = Type.NULL;
            _parameters = (new ImmutableListBuilder<TypedParameter>(0)).build();
        }
        else {
            _type = (typedParameters.length == 1 ? Type.SINGLE : Type.TUPLE);

            final ImmutableListBuilder<TypedParameter> typedParameterListBuilder = new ImmutableListBuilder<TypedParameter>(typedParameters.length);
            for (final TypedParameter typedParameter : typedParameters) {
                typedParameterListBuilder.add(typedParameter);
            }
            _parameters = typedParameterListBuilder.build();
        }
    }

    public Type getType() {
        return _type;
    }

    public Integer getParameterCount() {
        return _parameters.getCount();
    }

    public TypedParameter getParameter() {
        if (_type != Type.SINGLE) {
            throw new RuntimeException("Attempted to get single parameter of tuple type.");
        }

        return _parameters.get(0);
    }

    public List<TypedParameter> getParameters() {
        return _parameters;
    }
}
