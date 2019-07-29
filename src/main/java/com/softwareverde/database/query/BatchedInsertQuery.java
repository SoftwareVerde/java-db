package com.softwareverde.database.query;

public class BatchedInsertQuery extends Query {
    public BatchedInsertQuery(final String query) {
        super(query);
    }

    @Override
    public String getQueryString() {
        final Integer parameterCount = _parameters.getSize();
        final Integer parameterCountPerBatch = (_query.length() - _query.replace("?", "").length());
        final int batchCount = (parameterCount / parameterCountPerBatch);

        if ( (batchCount > 0) && ((parameterCount % parameterCountPerBatch) != 0) ) {
            throw new RuntimeException("Invalid parameter count for batched query: " + _query);
        }

        final StringBuilder stringBuilder = new StringBuilder(_query);

        for (int i = 1; i < batchCount; ++i) {
            stringBuilder.append(", (");
            for (int j = 0; j < parameterCountPerBatch; ++j) {
                stringBuilder.append("?");
                if (j < parameterCountPerBatch - 1) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append(")");
        }

        return stringBuilder.toString();
    }

    public void clear() {
        _parameters.clear();
    }
}
