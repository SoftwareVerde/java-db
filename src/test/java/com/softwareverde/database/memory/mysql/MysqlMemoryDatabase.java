package com.softwareverde.database.memory.mysql;

import com.softwareverde.database.Database;
import com.softwareverde.database.DatabaseConnection;
import com.softwareverde.database.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlMemoryDatabase implements Database<Connection> {
    protected static Integer _nextSequenceNumber = 0;

    protected final Integer _sequenceNumber;
    protected Connection _connect() throws SQLException, ClassNotFoundException {
        Class.forName("org.h2.Driver");
        return org.h2.Driver.load().connect("jdbc:h2:mem:db"+ _sequenceNumber +";MODE=MYSQL", null);
    }

    public MysqlMemoryDatabase() {
        _sequenceNumber = _nextSequenceNumber;
        _nextSequenceNumber += 1;
    }

    @Override
    public DatabaseConnection<Connection> newConnection() throws DatabaseException {
        try {
            final Connection connection = _connect();
            return new MysqlMemoryDatabaseConnection(connection);
        }
        catch (SQLException | ClassNotFoundException exception) {
            throw new DatabaseException("Unable to connect to _database.", exception);
        }
    }

    /**
     * Require dependencies be packaged at compile-time.
     */
    private static final Class[] UNUSED = {
            org.h2.Driver.class
    };
}