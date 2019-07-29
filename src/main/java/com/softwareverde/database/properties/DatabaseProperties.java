package com.softwareverde.database.properties;

public interface DatabaseProperties {
    String getRootPassword();
    String getHostname();
    String getUsername();
    String getPassword();
    String getSchema();
    Integer getPort();

    DatabaseCredentials getRootCredentials();
    DatabaseCredentials getCredentials();
}
