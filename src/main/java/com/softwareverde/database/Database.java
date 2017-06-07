package com.softwareverde.database;

import java.util.List;

public interface Database {
    interface Row {
        List<String> getColumnNames();
        String getValue(String columnName);
    }

    void connect();
    Boolean isConnected();
    void disconnect();

    void executeDdl(String query);
    Long executeSql(String query, String[] parameters);
    List<Row> query(String query, String[] parameters);

    Integer getVersion();
    void setVersion(Integer newVersion);

    Boolean shouldBeCreated();
    Boolean shouldBeUpgraded();
    Boolean shouldBeDowngraded();

    Database newConnection();
}
