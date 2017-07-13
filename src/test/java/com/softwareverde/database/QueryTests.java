package com.softwareverde.database;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QueryTests {

    @Test
    public void should_add_multiple_parameter_types_to_query() {
        // Setup
        final Query query = new Query("SELECT ?, ?, ?, ?, ?");
        final Boolean booleanObject = true;
        final Integer integerObject = -2;

        // Action
        query.setParameter(true);
        query.setParameter(booleanObject);
        query.setParameter("String");
        query.setParameter(2);
        query.setParameter(integerObject);

        // Assert
        final List<String> parameterValues = query.getParameters();
        Assert.assertEquals("1", parameterValues.get(0));
        Assert.assertEquals("1", parameterValues.get(1));
        Assert.assertEquals("String", parameterValues.get(2));
        Assert.assertEquals("2", parameterValues.get(3));
        Assert.assertEquals("-2", parameterValues.get(4));
    }
}
