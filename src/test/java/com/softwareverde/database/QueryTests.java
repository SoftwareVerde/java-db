package com.softwareverde.database;

import com.softwareverde.constable.list.List;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;
import org.junit.Assert;
import org.junit.Test;

public class QueryTests {

    @Test
    public void should_add_multiple_parameter_types_to_query() {
        // Setup
        final Query query = new Query("SELECT ?, ?, ?, ?, ?, ?");
        final Boolean booleanObject = true;
        final Integer integerObject = -2;

        // Action
        query.setParameter(true);
        query.setParameter(booleanObject);
        query.setParameter("String");
        query.setParameter(2);
        query.setParameter(integerObject);
        query.setParameter(new byte[] { 0x00, 0x01 });

        // Assert
        final List<TypedParameter> parameterValues = query.getParameters();

        Assert.assertEquals(true, parameterValues.get(0).value);
        Assert.assertEquals(ParameterType.BOOLEAN, parameterValues.get(0).type);

        Assert.assertEquals(true, parameterValues.get(1).value);
        Assert.assertEquals(ParameterType.BOOLEAN, parameterValues.get(1).type);

        Assert.assertEquals("String", parameterValues.get(2).value);
        Assert.assertEquals(ParameterType.STRING, parameterValues.get(2).type);

        Assert.assertEquals("2", parameterValues.get(3).value);
        Assert.assertEquals(ParameterType.STRING, parameterValues.get(3).type);

        Assert.assertEquals("-2", parameterValues.get(4).value);
        Assert.assertEquals(ParameterType.STRING, parameterValues.get(4).type);

        Assert.assertEquals(2, ((byte[]) parameterValues.get(5).value).length);
        Assert.assertEquals(ParameterType.BYTE_ARRAY, parameterValues.get(5).type);
        Assert.assertEquals((byte) 0x00, ((byte[]) parameterValues.get(5).value)[0]);
        Assert.assertEquals((byte) 0x01, ((byte[]) parameterValues.get(5).value)[1]);
    }
}
