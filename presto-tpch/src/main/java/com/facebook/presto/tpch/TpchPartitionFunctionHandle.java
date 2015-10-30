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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TpchPartitionFunctionHandle
        implements ConnectorPartitionFunctionHandle
{
    private final String table;
    private final int totalRows;

    @JsonCreator
    public TpchPartitionFunctionHandle(@JsonProperty("table") String table, @JsonProperty("totalRows") int totalRows)
    {
        this.table = table;
        this.totalRows = totalRows;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public int getTotalRows()
    {
        return totalRows;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TpchPartitionFunctionHandle that = (TpchPartitionFunctionHandle) o;
        return Objects.equals(table, that.table) &&
                totalRows == that.totalRows;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, totalRows);
    }

    @Override
    public String toString()
    {
        return table + ":" + totalRows;
    }
}
