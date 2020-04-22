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
    public static final String TRUE_STRING = "TRUE";

    protected static final Pattern IN_CLAUSE_PATTERN = Pattern.compile("((\\([^()]+\\))|(\\w+))? IN[ ]*\\(\\?\\)");
    protected static final Pattern TUPLE_PATTERN = Pattern.compile("([^,()\\s]+)");

    protected static class ExtendedInClauseParameters {
        public final List<InClauseParameter> inClauseParameters;
        public final Boolean isExtendedInClause;

        public ExtendedInClauseParameters(final List<InClauseParameter> inClauseParameters, final Boolean isExtendedInClause) {
            this.inClauseParameters = inClauseParameters;
            this.isExtendedInClause = isExtendedInClause;
        }
    }

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

    protected static String buildAndList(final List<String> columnNames) {
        final StringBuilder stringBuilder = new StringBuilder();

        String separator = "";
        for (final String columnName : columnNames) {
            stringBuilder.append(separator);
            stringBuilder.append(columnName);
            stringBuilder.append(" = ?");
            separator = " AND ";
        }

        return stringBuilder.toString();
    }

    protected static String buildExpandedWhereInClause(final List<String> columnNames, final Integer tupleCount) {
        if (tupleCount == 0) { return TRUE_STRING; }

        final StringBuilder stringBuilder = new StringBuilder();
        String separator = "";
        for (int i = 0; i < tupleCount; ++i) {
            stringBuilder.append(separator);
            stringBuilder.append("(");

            stringBuilder.append(Query.buildAndList(columnNames));

            stringBuilder.append(")");

            separator = " OR ";
        }
        return stringBuilder.toString();
    }

    protected final String _query;
    protected final MutableList<TypedParameter> _parameters;
    protected final ParameterFactory _parameterFactory;

    protected final MutableList<ExtendedInClauseParameters> _inClauseParameters = new MutableList<ExtendedInClauseParameters>();
    protected final MutableList<Integer> _inClauseParameterIndexes = new MutableList<Integer>();
    protected Integer _nextParameterIndex = 0;

    protected void _setInClauseParameters(final InClauseParameter inClauseParameter, final InClauseParameter[] extraInClauseParameters, final Boolean enableExpandedInClause) {
        final MutableList<InClauseParameter> valueTuples = new MutableList<InClauseParameter>((extraInClauseParameters != null ? extraInClauseParameters.length : 0) + 1);
        valueTuples.add(Util.coalesce(inClauseParameter, InClauseParameter.NULL));

        if (extraInClauseParameters != null) {
            for (final InClauseParameter extraInClauseParameter : extraInClauseParameters) {
                valueTuples.add(extraInClauseParameter);
            }
        }

        _inClauseParameters.add(new ExtendedInClauseParameters(valueTuples, enableExpandedInClause));
        _inClauseParameterIndexes.add(_nextParameterIndex);
        _nextParameterIndex += 1;
    }

    protected  <T> void _setInClauseParameters(final Iterable<? extends T> values, final ValueExtractor<T> valueExtractor, final Boolean enableExpandedInClause) {
        final MutableList<InClauseParameter> typedParameters = new MutableList<InClauseParameter>();
        for (final T value : values) {
            final InClauseParameter inClauseParameter = valueExtractor.extractValues(value);
            typedParameters.add(Util.coalesce(inClauseParameter, InClauseParameter.NULL));
        }

        _inClauseParameters.add(new ExtendedInClauseParameters(typedParameters, enableExpandedInClause));
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
        _setInClauseParameters(inClauseParameter, extraInClauseParameters, false);
        return this;
    }

    public <T> Query setInClauseParameters(final Iterable<? extends T> values, final ValueExtractor<T> valueExtractor) {
        _setInClauseParameters(values, valueExtractor, false);
        return this;
    }

    public <T> Query setExpandedInClauseParameters(final InClauseParameter inClauseParameter, final InClauseParameter... extraInClauseParameters) {
        _setInClauseParameters(inClauseParameter, extraInClauseParameters, true);
        return this;
    }

    public <T> Query setExpandedInClauseParameters(final Iterable<? extends T> values, final ValueExtractor<T> valueExtractor) {
        _setInClauseParameters(values, valueExtractor, true);
        return this;
    }

    public String getQueryString() {
        final int inClauseCount = _inClauseParameters.getCount();
        if (inClauseCount == 0) {
            return _query;
        }

        final Matcher matcher = Query.IN_CLAUSE_PATTERN.matcher(_query);

        final StringBuffer stringBuffer = new StringBuffer();
        {
            int matchCount = 0;
            while (matcher.find()) {
                if (matchCount >= inClauseCount) {
                    break;
                }

                final ExtendedInClauseParameters extendedInClauseParameters = _inClauseParameters.get(matchCount);
                final List<InClauseParameter> inClauseParameters = extendedInClauseParameters.inClauseParameters;
                final Boolean useExpandedInClause = extendedInClauseParameters.isExtendedInClause;

                final String rawColumnNames = matcher.group(1);
                if (useExpandedInClause) {
                    final Matcher columnNameMatcher = Query.TUPLE_PATTERN.matcher(rawColumnNames);

                    final MutableList<String> columnNames = new MutableList<String>();
                    while (columnNameMatcher.find()) {
                        final String columnName = columnNameMatcher.group(1);
                        columnNames.add(columnName);
                    }

                    final String inClause = "(" + Query.buildExpandedWhereInClause(columnNames, inClauseParameters.getCount()) + ")";
                    matcher.appendReplacement(stringBuffer, inClause);
                }
                else {
                    final String inClause = Query.buildInClause(inClauseParameters);
                    matcher.appendReplacement(stringBuffer, (rawColumnNames + " " + inClause));
                }

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
                final ExtendedInClauseParameters extendedInClauseParameters = _inClauseParameters.get(inClauseIndex);
                for (final InClauseParameter inClauseParameter : extendedInClauseParameters.inClauseParameters) {
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
