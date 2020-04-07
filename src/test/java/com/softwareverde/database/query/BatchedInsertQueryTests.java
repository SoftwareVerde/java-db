package com.softwareverde.database.query;

import org.junit.Assert;
import org.junit.Test;

public class BatchedInsertQueryTests {

    @Test
    public void should_create_batched_query_with_batch_size_1() {
        // Setup
        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?)");
        batchedInsertQuery.setParameter("valueA0");
        batchedInsertQuery.setParameter("valueB1");
        batchedInsertQuery.setParameter("valueC2");

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?)", query);
    }

    @Test
    public void should_create_batched_query_with_batch_size_2() {
        // Setup
        final int batchSize = 2;

        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?)");

        for (int i = 0; i < batchSize; ++i) {
            batchedInsertQuery.setParameter("valueA" + i);
            batchedInsertQuery.setParameter("valueB" + i);
            batchedInsertQuery.setParameter("valueC" + i);
        }

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?), (?, ?, ?)", query);
    }

    @Test
    public void should_create_batched_query_with_batch_size_7() {
        // Setup
        final int batchSize = 7;

        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?)");

        for (int i = 0; i < batchSize; ++i) {
            batchedInsertQuery.setParameter("valueA" + i);
            batchedInsertQuery.setParameter("valueB" + i);
            batchedInsertQuery.setParameter("valueC" + i);
        }

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1, column2) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)", query);
    }

    @Test
    public void should_create_batched_query_supporting_on_duplicate_key_update() {
        // Setup
        final int batchSize = 3;

        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1) VALUES (?, ?) ON DUPLICATE KEY UPDATE column1 = NULL");

        for (int i = 0; i < batchSize; ++i) {
            batchedInsertQuery.setParameter("valueA" + i);
            batchedInsertQuery.setParameter("valueB" + i);
        }

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1) VALUES (?, ?), (?, ?), (?, ?) ON DUPLICATE KEY UPDATE column1 = NULL", query);
    }

    @Test
    public void should_create_batched_query_with_static_parameter() {
        // Setup
        final int batchSize = 3;

        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1, column2) VALUES (?, ?, 1)");

        for (int i = 0; i < batchSize; ++i) {
            batchedInsertQuery.setParameter("valueA" + i);
            batchedInsertQuery.setParameter("valueB" + i);
        }

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1, column2) VALUES (?, ?, 1), (?, ?, 1), (?, ?, 1)", query);
    }

    @Test
    public void should_create_batched_query_with_batch_size_7_with_static_parameter_and_duplicate_update_clause() {
        // Setup
        final int batchSize = 7;

        final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO table (column0, column1, column2) VALUES (?, 1, ?) ON DUPLICATE KEY UPDATE column1 = VALUE (column1)");

        for (int i = 0; i < batchSize; ++i) {
            batchedInsertQuery.setParameter("valueA" + i);
            batchedInsertQuery.setParameter("valueC" + i);
        }

        // Action
        final String query = batchedInsertQuery.getQueryString();

        // Assert
        Assert.assertEquals("INSERT INTO table (column0, column1, column2) VALUES (?, 1, ?), (?, 1, ?), (?, 1, ?), (?, 1, ?), (?, 1, ?), (?, 1, ?), (?, 1, ?) ON DUPLICATE KEY UPDATE column1 = VALUE (column1)", query);
    }
}
