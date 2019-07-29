package com.softwareverde.database;

import com.softwareverde.constable.list.List;
import com.softwareverde.database.query.Query;
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
        final Float floatObject = 7.777F;

        // Action
        query.setParameter(true);                           // 0
        query.setParameter(booleanObject);                  // 1
        query.setParameter("String");                       // 2
        query.setParameter(2);                              // 3
        query.setParameter(integerObject);                  // 4
        query.setParameter(new byte[] { 0x00, 0x01 });      // 5
        query.setParameter(floatObject);                    // 6
        query.setParameter(7F);                             // 7

        // Assert
        final List<TypedParameter> parameterValues = query.getParameters();

        Assert.assertEquals(1L, parameterValues.get(0).value);
        Assert.assertEquals(ParameterType.WHOLE_NUMBER, parameterValues.get(0).type);

        Assert.assertEquals(1L, parameterValues.get(1).value);
        Assert.assertEquals(ParameterType.WHOLE_NUMBER, parameterValues.get(1).type);

        Assert.assertEquals("String", parameterValues.get(2).value);
        Assert.assertEquals(ParameterType.STRING, parameterValues.get(2).type);

        Assert.assertEquals(2L, parameterValues.get(3).value);
        Assert.assertEquals(ParameterType.WHOLE_NUMBER, parameterValues.get(3).type);

        Assert.assertEquals(-2L, parameterValues.get(4).value);
        Assert.assertEquals(ParameterType.WHOLE_NUMBER, parameterValues.get(4).type);

        Assert.assertEquals(2, ((byte[]) parameterValues.get(5).value).length);
        Assert.assertEquals(ParameterType.BYTE_ARRAY, parameterValues.get(5).type);
        Assert.assertEquals((byte) 0x00, ((byte[]) parameterValues.get(5).value)[0]);
        Assert.assertEquals((byte) 0x01, ((byte[]) parameterValues.get(5).value)[1]);

        Assert.assertEquals(7.777D, ((Double) parameterValues.get(6).value), 0.0001F);
        Assert.assertEquals(ParameterType.FLOATING_POINT_NUMBER, parameterValues.get(6).type);

        Assert.assertEquals(7D, parameterValues.get(7).value);
        Assert.assertEquals(ParameterType.FLOATING_POINT_NUMBER, parameterValues.get(7).type);
    }
}
