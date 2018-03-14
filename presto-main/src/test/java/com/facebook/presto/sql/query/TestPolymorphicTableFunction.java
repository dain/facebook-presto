/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPolymorphicTableFunction
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void test()
    {
        assertions.assertQuery(
                "SELECT a, b FROM TABLE (\n" +
                        "    split_column(\n" +
                        "        split_column => DESCRIPTOR (x), \n" +
                        "        delimiter => ',', \n" +
                        "        input => TABLE (" +
                        "           SELECT * " +
                        "           FROM (VALUES ('foo,bar,1,2')) t(x)" +
                        "        ),\n" +
                        "        output => DESCRIPTOR (A VARCHAR, B VARCHAR, C BIGINT, D BIGINT)))",
                "VALUES ('foo', 'bar')");
    }
}
