package com.softwareverde.database.row;

import java.util.List;

public interface Row {
    List<String> getColumnNames();

    String getString(String columnName);
    Integer getInteger(String columnName);
    Long getLong(String columnName);
    Float getFloat(String columnName);
    Double getDouble(String columnName);
    Boolean getBoolean(String columnName);
    byte[] getBytes(String columnName);
}
