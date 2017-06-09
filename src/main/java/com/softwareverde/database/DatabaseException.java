package com.softwareverde.database;

public class DatabaseException extends Exception {
    public DatabaseException(final String message) {
        super(message);
    }

    public DatabaseException(final Exception baseException) {
        super(baseException);
    }

    public DatabaseException(final String message, final Exception baseException) {
        super(message, baseException);
    }
}
