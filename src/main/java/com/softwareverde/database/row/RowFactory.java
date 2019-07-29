package com.softwareverde.database.row;

import com.softwareverde.database.DatabaseException;

import java.sql.ResultSet;

public interface RowFactory {
    Row fromResultSet(final ResultSet resultSet) throws DatabaseException;
}
