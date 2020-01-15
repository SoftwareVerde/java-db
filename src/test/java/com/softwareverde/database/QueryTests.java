package com.softwareverde.database;

import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.query.Query;
import com.softwareverde.database.query.ValueExtractor;
import com.softwareverde.database.query.parameter.InClauseParameter;
import com.softwareverde.database.query.parameter.ParameterType;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void should_create_query_with_in_clause() {
        // Setup
        final Query query = new Query("SELECT * FROM rows WHERE value = ? AND id IN(?)");

        final int itemCount = 3;
        final List<String> items = new ArrayList<String>(itemCount);
        for (int i = 0; i < itemCount; ++i) {
            items.add("" + i);
        }

        // Action
        query.setParameter("value");
        query.setInClauseParameters(items, ValueExtractor.STRING);
        final String queryString = query.getQueryString();
        final List<TypedParameter> parameters = query.getParameters();

        // Assert
        Assert.assertEquals("SELECT * FROM rows WHERE value = ? AND id IN (?, ?, ?)", queryString);
        Assert.assertEquals((itemCount + 1), parameters.size());
        Assert.assertEquals("value", parameters.get(0).value);
        Assert.assertEquals("0", parameters.get(1).value);
        Assert.assertEquals("1", parameters.get(2).value);
        Assert.assertEquals("2", parameters.get(3).value);
    }

    @Test
    public void should_create_query_with_in_clause_using_null() {
        // Setup
        final Query query = new Query("SELECT * FROM rows WHERE value = ? AND id IN(?)");

        // Action
        query.setParameter("value");
        query.setInClauseParameters(null);
        final String queryString = query.getQueryString();
        final List<TypedParameter> parameters = query.getParameters();

        // Assert
        Assert.assertEquals("SELECT * FROM rows WHERE value = ? AND id IN (?)", queryString);
        Assert.assertEquals(2, parameters.size());
        Assert.assertEquals("value", parameters.get(0).value);
        Assert.assertEquals(TypedParameter.NULL, parameters.get(1));
    }

    @Test
    public void should_create_query_with_multiple_in_clauses() {
        // Setup
        final Query query = new Query("SELECT * FROM rows WHERE value = ? AND ( id IN(?) OR id IN (?) )");

        final int firstItemCount = 3;
        final List<String> firstItems = new ArrayList<String>(firstItemCount);
        for (int i = 0; i < firstItemCount; ++i) {
            firstItems.add("" + i);
        }

        final int secondItemCount = 4;
        final List<String> secondItems = new ArrayList<String>(secondItemCount);
        for (int i = 0; i < secondItemCount; ++i) {
            secondItems.add("" + i);
        }

        // Action
        query.setParameter("value");
        query.setInClauseParameters(firstItems, ValueExtractor.STRING);
        query.setInClauseParameters(secondItems, ValueExtractor.STRING);
        final String queryString = query.getQueryString();
        final List<TypedParameter> parameters = query.getParameters();

        // Assert
        Assert.assertEquals("SELECT * FROM rows WHERE value = ? AND ( id IN (?, ?, ?) OR id IN (?, ?, ?, ?) )", queryString);
        Assert.assertEquals((firstItemCount + secondItemCount + 1), parameters.size());
        Assert.assertEquals("value", parameters.get(0).value);
        Assert.assertEquals("0", parameters.get(1).value);
        Assert.assertEquals("1", parameters.get(2).value);
        Assert.assertEquals("2", parameters.get(3).value);
        Assert.assertEquals("0", parameters.get(4).value);
        Assert.assertEquals("1", parameters.get(5).value);
        Assert.assertEquals("2", parameters.get(6).value);
        Assert.assertEquals("3", parameters.get(7).value);
    }

    @Test
    public void should_create_query_of_multiple_tuple_pairs() {
        // Setup
        final Query query = new Query("SELECT id FROM table WHERE (value0, value1) IN (?)");

        final ValueExtractor<Tuple<String, String>> stringTupleExtractor = new ValueExtractor<Tuple<String, String>>() {
            @Override
            public InClauseParameter extractValues(final Tuple<String, String> tuple) {
                final TypedParameter typedParameter0 = new TypedParameter(tuple.first);
                final TypedParameter typedParameter1 = new TypedParameter(tuple.second);
                return new InClauseParameter(typedParameter0, typedParameter1);
            }
        };

        final MutableList<Tuple<String, String>> values = new MutableList<Tuple<String, String>>();
        for (int i = 0; i < 3; ++i) {
            values.add(new Tuple<String, String>("Value0-" + i, "Value1-" + i));
        }

        // Action
        query.setInClauseParameters(values, stringTupleExtractor);

        final String queryString = query.getQueryString();

        // Assert
        Assert.assertEquals("SELECT id FROM table WHERE (value0, value1) IN ((?,?), (?,?), (?,?))", queryString);
    }
}
