package com.facebook.presto.execution;

import org.testng.annotations.Test;

import static com.facebook.presto.execution.FunctionUtils.decodeFunctionSessionProperty;
import static com.facebook.presto.execution.FunctionUtils.encodeFunctionSessionProperty;
import static org.testng.Assert.assertEquals;

public class TestFunctionUtils
{
    private static final String FUNCTION = "CREATE FUNCTION test(a bigint) RETURNS bigint BEGIN DECLARE x bigint DEFAULT 99; RETURN x * a; END";

    @Test
    public void testRoundTrip()
            throws Exception
    {
        assertEquals(decodeFunctionSessionProperty(encodeFunctionSessionProperty(FUNCTION)), FUNCTION);
    }
}
