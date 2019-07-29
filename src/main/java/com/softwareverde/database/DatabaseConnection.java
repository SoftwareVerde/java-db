package com.softwareverde.database;

import com.softwareverde.database.query.Query;
import com.softwareverde.database.row.Row;

import java.util.List;

public interface DatabaseConnection<T> extends AutoCloseable {

    void executeDdl(final String query) throws DatabaseException;
    void executeDdl(final Query query) throws DatabaseException;

    /**
     * The insertId is returned, if applicable.
     * Otherwise, a zero is returned.
     */
    Long executeSql(final String query, final String[] parameters) throws DatabaseException;
    Long executeSql(final Query query) throws DatabaseException;

    /**
     * Returns a list of rows upon success.
     */
    List<Row> query(final String query, final String[] parameters) throws DatabaseException;
    List<Row> query(final Query query) throws DatabaseException;

    void close() throws DatabaseException;

    /**
     * Returns the underlying Sql Connection.
     */
    T getRawConnection();

}