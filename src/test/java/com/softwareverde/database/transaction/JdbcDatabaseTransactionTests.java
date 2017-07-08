package com.softwareverde.database.transaction;

import com.softwareverde.database.*;
import com.softwareverde.database.memory.mysql.MysqlMemoryDatabase;
import com.softwareverde.util.Container;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

class SingleConnectionDatabase extends MysqlMemoryDatabase {
    protected final Database<Connection> _database;
    protected final DatabaseConnection<Connection> _keepAliveDatabaseConnection; // Used to prevent the _database from being closed.
    protected final DatabaseConnection<Connection> _databaseConnection;
    protected final List<DatabaseConnection<Connection>> _allConnections = new LinkedList<DatabaseConnection<Connection>>();

    public SingleConnectionDatabase() throws DatabaseException {
        _database = new MysqlMemoryDatabase();
        _keepAliveDatabaseConnection = _database.newConnection();
        _databaseConnection = _database.newConnection();

        _allConnections.add(_keepAliveDatabaseConnection);
        _allConnections.add(_databaseConnection);
    }

    @Override
    public DatabaseConnection<Connection> newConnection() throws DatabaseException {
        return _databaseConnection;
    }

    public Connection getRawDatabaseConnection() {
        return _databaseConnection.getRawConnection();
    }

    public DatabaseConnection<Connection> actuallyGetNewConnection() throws DatabaseException {
        final DatabaseConnection<Connection> newConnection = _database.newConnection();
        _allConnections.add(newConnection);
        return newConnection;
    }

    public void initTables() throws DatabaseException {
        _databaseConnection.executeDdl("CREATE TABLE test_table (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, test_column VARCHAR(255) NOT NULL)");
    }

    public void destroy() throws SQLException {
        for (final DatabaseConnection<Connection> databaseConnection : _allConnections) {
            final Connection rawConnection = databaseConnection.getRawConnection();
            if (! rawConnection.isClosed()) {
                rawConnection.close();
            }
        }
    }
}

public class JdbcDatabaseTransactionTests {
    protected SingleConnectionDatabase _database;

    @Before
    public void setUp() throws Exception {
         _database = new SingleConnectionDatabase();
         _database.initTables();
    }

    @After
    public void tearDown() throws Exception {
        _database.destroy();
    }

    @Test
    public void should_execute_sql_within_transaction() throws Exception {
        // Setup
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(_database);

        // Action
        databaseTransaction.execute(new DatabaseRunnable<Connection>() {
            @Override
            public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                databaseConnection.executeSql(new Query("INSERT INTO test_table (test_column) VALUES (?)").setParameter("value"));
            }
        });

        // Assert
        final Row row = _database.actuallyGetNewConnection().query("SELECT * FROM test_table", null).get(0);
        Assert.assertEquals("value", row.getString("test_column"));
    }

    @Test
    public void should_have_closed_database_connection_on_successful_transaction() throws Exception {
        // Setup
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(_database);

        // Action
        databaseTransaction.execute(new DatabaseRunnable<Connection>() {
            @Override
            public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                databaseConnection.executeSql(new Query("INSERT INTO test_table (test_column) VALUES (?)").setParameter("value"));
            }
        });

        // Assert
        Assert.assertTrue(_database.getRawDatabaseConnection().isClosed());
    }

    @Test
    public void should_have_closed_database_connection_on_unsuccessful_transaction() throws Exception {
        // Setup
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(_database);

        // Action
        try {
            databaseTransaction.execute(new DatabaseRunnable<Connection>() {
                @Override
                public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                    databaseConnection.executeSql(new Query("INVALID QUERY: SYNTAX ERROR").setParameter("value"));
                }
            });
        }
        catch (final DatabaseException databaseException) { }

        // Assert
        Assert.assertTrue(_database.getRawDatabaseConnection().isClosed());
    }

    @Test
    public void should_have_closed_database_connection_on_empty_transaction() throws Exception {
        // Setup
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(_database);

        // Action
        databaseTransaction.execute(new DatabaseRunnable<Connection>() {
            @Override
            public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                // Nothing.
            }
        });

        // Assert
        Assert.assertTrue(_database.getRawDatabaseConnection().isClosed());
    }

    @Test
    public void should_rollback_statements_if_one_fails() throws Exception {
        // Setup
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(_database);

        try (final DatabaseConnection<Connection> databaseConnection = _database.actuallyGetNewConnection()) {
            databaseConnection.executeSql(new Query("INSERT INTO test_table (test_column) VALUES (?)").setParameter("original value"));
        }

        final Container<String> midTransactionValue = new Container<String>();

        // Action
        try {
            databaseTransaction.execute(new DatabaseRunnable<Connection>() {
                @Override
                public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                    databaseConnection.executeSql(new Query("UPDATE test_table SET test_column = ?").setParameter("new value"));

                    final Row row = databaseConnection.query("SELECT * FROM test_table", null).get(0);
                    midTransactionValue.value = row.getString("test_column");

                    databaseConnection.executeSql(new Query("INVALID QUERY: SYNTAX ERROR"));
                }
            });
        }
        catch (final DatabaseException databaseException) { }

        // Assert
        Assert.assertEquals("new value", midTransactionValue.value); // Assert the initial update was successful before the rollback...

        try (final DatabaseConnection<Connection> databaseConnection = _database.actuallyGetNewConnection()) {
            final Row row = databaseConnection.query("SELECT * FROM test_table", null).get(0);
            Assert.assertEquals("original value", row.getString("test_column"));
        }
    }
}