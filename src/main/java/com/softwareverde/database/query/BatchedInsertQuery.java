package com.softwareverde.database.query;

import com.softwareverde.database.query.parameter.ParameterFactory;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BatchedInsertQuery extends Query {
    protected static final HashMap<Integer, Pattern> PARAMETER_MATCHERS = new HashMap<Integer, Pattern>();

    protected static ReentrantReadWriteLock.ReadLock PARAMETER_MATCHER_READ_LOCK;
    protected static ReentrantReadWriteLock.WriteLock PARAMETER_MATCHER_WRITE_LOCK;
    static {
        final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        PARAMETER_MATCHER_READ_LOCK = readWriteLock.readLock();
        PARAMETER_MATCHER_WRITE_LOCK = readWriteLock.writeLock();
    }

    protected static Pattern buildMatcher(final Integer parameterCount) {
        final String parameterMatchString;
        {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(");
            String separator = "";
            for (int i = 0; i < parameterCount; ++i) {
                stringBuilder.append(separator);
                stringBuilder.append("\\?");
                separator = ",[ ]*";
            }
            stringBuilder.append(")");

            parameterMatchString = stringBuilder.toString();
        }

        return Pattern.compile(parameterMatchString);
    }

    protected static Pattern getMatcher(final Integer parameterCount) {
        if (parameterCount > 64) {
            // Do not cache inserts with columns greater than 64 entries.
            //  This limit is fairly arbitrary and only intended to handle forever-caching unpredicted one-off queries with large column counts.
            return BatchedInsertQuery.buildMatcher(parameterCount);
        }

        PARAMETER_MATCHER_READ_LOCK.lock();
        try {
            final Pattern pattern = PARAMETER_MATCHERS.get(parameterCount);
            if (pattern != null) { return pattern; }
        }
        finally {
            PARAMETER_MATCHER_READ_LOCK.unlock();
        }

        PARAMETER_MATCHER_WRITE_LOCK.lock();
        try {
            final Pattern pattern = BatchedInsertQuery.buildMatcher(parameterCount);
            PARAMETER_MATCHERS.put(parameterCount, pattern);
            return pattern;
        }
        finally {
            PARAMETER_MATCHER_WRITE_LOCK.unlock();
        }
    }

    protected BatchedInsertQuery(final Query query, final Boolean shouldConsumeQuery, final ParameterFactory parameterFactory) {
        super(query, shouldConsumeQuery, parameterFactory);
    }

    public BatchedInsertQuery(final String query) {
        super(query);
    }

    public BatchedInsertQuery(final Query query) {
        super(query);
    }

    @Override
    public String getQueryString() {
        final int parameterCount = _parameters.getCount();
        final int parameterCountPerBatch = (_query.length() - _query.replace("?", "").length());
        final int batchCount = (parameterCount / parameterCountPerBatch);

        if ( (batchCount > 0) && ((parameterCount % parameterCountPerBatch) != 0) ) {
            throw new RuntimeException("Invalid parameter count for batched query: " + _query);
        }

        final int writeIndex;
        {
            final Pattern pattern = BatchedInsertQuery.getMatcher(parameterCountPerBatch);
            final Matcher matcher = pattern.matcher(_query);
            if (matcher.find()) {
                writeIndex = (matcher.start() - 1);
            }
            else {
                writeIndex = _query.length();
            }
        }

        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(_query, 0, writeIndex);

        for (int i = 1; i < batchCount; ++i) {
            stringBuilder.append("(");
            for (int j = 0; j < parameterCountPerBatch; ++j) {
                stringBuilder.append("?");
                if (j < parameterCountPerBatch - 1) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append("), ");
        }

        stringBuilder.append(_query.substring(writeIndex));

        return stringBuilder.toString();
    }

    public void clear() {
        _parameters.clear();
    }
}
