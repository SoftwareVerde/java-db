package com.softwareverde.database.query;

import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.query.parameter.InClauseParameter;
import com.softwareverde.database.query.parameter.ParameterFactory;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.Util;
import com.softwareverde.util.type.identifier.Identifier;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {
    public static final TypedParameter NULL = TypedParameter.NULL;
    public static final String NULL_STRING = "NULL";

    protected static StringBuilder buildParenthesisList(final Integer parameterCount) {
        final StringBuilder stringBuilder = new StringBuilder();

        if (parameterCount > 0) {
            stringBuilder.append("(");
            String separator = "";
            for (int i = 0; i < parameterCount; ++i) {
                stringBuilder.append(separator);
                stringBuilder.append("?");
                separator = ",";
            }
            stringBuilder.append(")");
        }
        else {
            stringBuilder.append(NULL_STRING);
        }

        return stringBuilder;
    }

    protected static String buildInClause(final List<InClauseParameter> inClauseParameters) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("IN (");

        if (inClauseParameters.getCount() > 0) {
            String separator = "";
            for (final InClauseParameter inClauseParameter : inClauseParameters) {
                stringBuilder.append(separator);
                if (inClauseParameter.getType() == InClauseParameter.Type.TUPLE) {
                    final Integer parameterCount = inClauseParameter.getParameterCount();
                    stringBuilder.append(Query.buildParenthesisList(parameterCount));
                }
                else {
                    stringBuilder.append("?");
                }

                separator = ", ";
            }
        }
        else {
            stringBuilder.append(NULL_STRING);
        }

        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    protected final String _query;
    protected final MutableList<TypedParameter> _parameters;
    protected final ParameterFactory _parameterFactory;

    protected final MutableList<List<InClauseParameter>> _inClauseParameters = new MutableList<List<InClauseParameter>>();
    protected final MutableList<Integer> _inClauseParameterIndexes = new MutableList<Integer>();
    protected Integer _nextParameterIndex = 0;

    protected void _setInClauseParameters(final InClauseParameter inClauseParameter, final InClauseParameter[] extraInClauseParameters) {
        final MutableList<InClauseParameter> valueTuples = new MutableList<InClauseParameter>((extraInClauseParameters != null ? extraInClauseParameters.length : 0) + 1);
        valueTuples.add(inClauseParameter);

        if (extraInClauseParameters != null) {
            for (final InClauseParameter extraInClauseParameter : extraInClauseParameters) {
                valueTuples.add(extraInClauseParameter);
            }
        }

        _inClauseParameters.add(valueTuples);
        _inClauseParameterIndexes.add(_nextParameterIndex);
        _nextParameterIndex += 1;
    }

    protected Query(final Query query, final Boolean shouldConsumeQuery, final ParameterFactory parameterFactory) {
        if (shouldConsumeQuery) {
            _query = query._query;
            _parameters = query._parameters;
        }
        else {
            _query = query._query;
            _parameters = new MutableList<TypedParameter>(query._parameters);
        }

        _parameterFactory = parameterFactory;
    }

    protected Query(final String query, final ParameterFactory parameterFactory) {
        _query = query;
        _parameters = new MutableList<TypedParameter>();
        _parameterFactory = parameterFactory;
    }

    public Query(final Query query) {
        this(query, false, new ParameterFactoryCore());
    }

    public Query(final String query) {
        _query = query;
        _parameters = new MutableList<TypedParameter>();
        _parameterFactory = new ParameterFactoryCore();
    }

    public Query setParameter(final Boolean value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Long value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Integer value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Short value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Double value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Float value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final byte[] value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final ByteArray value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final Identifier value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final TypedParameter value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setParameter(final String value) {
        final TypedParameter typedParameter = _parameterFactory.fromObject(value);
        _parameters.add(typedParameter);

        _nextParameterIndex += 1;
        return this;
    }

    public Query setNullParameter() {
        _parameters.add(NULL);
        _nextParameterIndex += 1;
        return this;
    }

    public Query setInClauseParameters(final InClauseParameter inClauseParameter, final InClauseParameter... extraInClauseParameters) {
        _setInClauseParameters(Util.coalesce(inClauseParameter, InClauseParameter.NULL), extraInClauseParameters);
        return this;
    }

    public <T> Query setInClauseParameters(final Iterable<? extends T> values, final ValueExtractor<T> valueExtractor) {
        final MutableList<InClauseParameter> typedParameters = new MutableList<InClauseParameter>();
        for (final T value : values) {
            final InClauseParameter inClauseParameter = valueExtractor.extractValues(value);
            typedParameters.add(Util.coalesce(inClauseParameter, InClauseParameter.NULL));
        }

        _inClauseParameters.add(typedParameters);
        _inClauseParameterIndexes.add(_nextParameterIndex);
        _nextParameterIndex += 1;
        return this;
    }

    public String getQueryString() {
        final int inClauseCount = _inClauseParameters.getCount();

        final Pattern pattern = Pattern.compile("IN[ ]*\\(\\?\\)");
        final Matcher matcher = pattern.matcher(_query);

        final StringBuffer stringBuffer = new StringBuffer();
        {
            int matchCount = 0;
            while (matcher.find()) {
                if (matchCount >= inClauseCount) {
                    break;
                }

                final List<InClauseParameter> inClauseValues = _inClauseParameters.get(matchCount);
                final String inClause = Query.buildInClause(inClauseValues);
                matcher.appendReplacement(stringBuffer, inClause);
                matchCount += 1;
            }
            matcher.appendTail(stringBuffer);

            if (matchCount != inClauseCount) {
                throw new RuntimeException("In Clause parameter mismatch. Expected " + matchCount + " found " + inClauseCount + ".");
            }
        }

        return stringBuffer.toString();
    }

    public java.util.List<TypedParameter> getParameters() {
        if (_inClauseParameters.isEmpty()) {
            final java.util.List<TypedParameter> parameters = new ArrayList<TypedParameter>(_parameters.getCount());
            for (final TypedParameter typedParameter : _parameters) {
                parameters.add(typedParameter);
            }
            return parameters;
        }

        final java.util.List<TypedParameter> parameters = new ArrayList<TypedParameter>();

        final int inClauseCount = _inClauseParameters.getCount();

        int inClauseIndex = 0;
        int nextInClauseParameterIndex = _inClauseParameterIndexes.get(inClauseIndex);
        int returnedParameterIndex = 0;
        int parameterIndex = 0;

        final int loopCount = (_parameters.getCount() + inClauseCount);
        for (int i = 0; i < loopCount; ++i) {
            if (nextInClauseParameterIndex == returnedParameterIndex) {
                for (final InClauseParameter inClauseParameter : _inClauseParameters.get(inClauseIndex)) {
                    if (inClauseParameter.getType() == InClauseParameter.Type.NULL) {
                        parameters.add(TypedParameter.NULL);
                    }
                    else {
                        for (final TypedParameter typedParameter : inClauseParameter.getParameters()) {
                            parameters.add(typedParameter);
                        }
                    }
                }

                inClauseIndex += 1;
                nextInClauseParameterIndex = (inClauseIndex < inClauseCount ? _inClauseParameterIndexes.get(inClauseIndex) : -1);
            }
            else {
                final TypedParameter parameter = _parameters.get(parameterIndex);
                parameters.add(parameter);
                parameterIndex += 1;
            }
            returnedParameterIndex += 1;
        }

        return parameters;
    }
}
