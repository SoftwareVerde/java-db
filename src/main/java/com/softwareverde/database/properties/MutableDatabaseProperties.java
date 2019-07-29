package com.softwareverde.database.properties;

public class MutableDatabaseProperties implements DatabaseProperties {
    protected String _rootPassword;
    protected String _hostname;
    protected String _username;
    protected String _password;
    protected String _schema;
    protected Integer _port;

    public MutableDatabaseProperties() { }

    public MutableDatabaseProperties(final DatabaseProperties databaseProperties) {
        _rootPassword = databaseProperties.getRootPassword();
        _hostname = databaseProperties.getHostname();
        _username = databaseProperties.getUsername();
        _password = databaseProperties.getPassword();
        _schema = databaseProperties.getSchema();
        _port = databaseProperties.getPort();
    }

    @Override
    public String getRootPassword() { return  _rootPassword; }

    @Override
    public String getHostname() { return _hostname; }

    @Override
    public String getUsername() { return _username; }

    @Override
    public String getPassword() { return _password; }

    @Override
    public String getSchema() { return _schema; }

    @Override
    public Integer getPort() { return _port; }

    @Override
    public DatabaseCredentials getRootCredentials() {
        return new DatabaseCredentials("root", _rootPassword);
    }

    @Override
    public DatabaseCredentials getCredentials() {
        return new DatabaseCredentials(_username, _password);
    }

    public void setRootPassword(final String rootPassword) { _rootPassword = rootPassword; }
    public void setHostname(final String hostname) { _hostname = hostname; }
    public void setUsername(final String username) { _username = username; }
    public void setPassword(final String password) { _password = password; }
    public void setSchema(final String schema) { _schema = schema; }
    public void setPort(final Integer port) { _port = port; }
}
