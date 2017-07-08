package com.softwareverde.database.transaction;

import com.softwareverde.database.*;
import com.softwareverde.database.mysqlmemorydatabase.MysqlMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;

class SingleConnectionDatabase extends MysqlMemoryDatabase {
    protected final Database<Connection> _database;
    protected final DatabaseConnection<Connection> _databaseConnection;

    public SingleConnectionDatabase() throws DatabaseException {
        _database = new MysqlMemoryDatabase();
        _databaseConnection = _database.newConnection();
        _databaseConnection.executeDdl("CREATE TABLE test_table (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, test_column VARCHAR(255) NOT NULL)");
    }

    @Override
    public DatabaseConnection<Connection> newConnection() throws DatabaseException {
        return _databaseConnection;
    }

    public Connection getRawDatabaseConnection() {
        return _databaseConnection.getRawConnection();
    }
}

public class JdbcDatabaseTransactionTests {

    @Test
    public void should_execute_sql_within_transaction() throws Exception {
        // Setup
        final SingleConnectionDatabase singleConnectionDatabase = new SingleConnectionDatabase();
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(singleConnectionDatabase);

        // Action
        databaseTransaction.execute(new DatabaseRunnable<Connection>() {
            @Override
            public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                databaseConnection.executeSql(new Query("INSERT INTO test_table (test_column) VALUES (?)").setParameter("value"));
            }
        });

        // Assert
        final Row row = singleConnectionDatabase.newConnection().query("SELECT * FROM test_table", null).get(0);
        Assert.assertEquals("value", row.getString("test_column"));
    }

    @Test
    public void should_have_closed_database_connection_on_successful_transaction() throws Exception {
        // Setup
        final SingleConnectionDatabase singleConnectionDatabase = new SingleConnectionDatabase();
        final DatabaseTransaction<Connection> databaseTransaction = new JdbcDatabaseTransaction(singleConnectionDatabase);

        // Action
        databaseTransaction.execute(new DatabaseRunnable<Connection>() {
            @Override
            public void run(final DatabaseConnection<Connection> databaseConnection) throws DatabaseException {
                databaseConnection.executeSql(new Query("INSERT INTO test_table (test_column) VALUES (?)").setParameter("value"));
            }
        });

        // Assert
        Assert.assertTrue(singleConnectionDatabase.getRawDatabaseConnection().isClosed());
    }
}