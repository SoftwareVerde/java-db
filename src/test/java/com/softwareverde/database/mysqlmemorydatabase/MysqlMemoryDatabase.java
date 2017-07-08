package com.softwareverde.database.mysqlmemorydatabase;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlMemoryDatabase implements Database<Connection> {

    protected Connection _connect() throws SQLException, ClassNotFoundException {
        Class.forName("org.h2.Driver");
        return org.h2.Driver.load().connect("jdbc:h2:mem:;MODE=MYSQL", null);
    }

    public MysqlMemoryDatabase() { }

    @Override
    public DatabaseConnection<Connection> newConnection() throws DatabaseException {
        try {
            final Connection connection = _connect();
            return new MysqlMemoryDatabaseConnection(connection);
        }
        catch (SQLException | ClassNotFoundException exception) {
            throw new DatabaseException("Unable to connect to database.", exception);
        }
    }

    /**
     * Require dependencies be packaged at compile-time.
     */
    private static final Class[] UNUSED = {
            org.h2.Driver.class
    };
}