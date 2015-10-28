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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.SystemPartitionFunctionId.hash;
import static com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.SystemPartitionFunctionId.roundRobin;
import static com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.SystemPartitionFunctionId.single;
import static java.util.Objects.requireNonNull;

public final class SystemPartitionFunctionHandle
        implements ConnectorPartitionFunctionHandle
{
    public static final PartitionFunctionHandle HASH = new PartitionFunctionHandle(Optional.empty(), new SystemPartitionFunctionHandle(hash));
    public static final PartitionFunctionHandle ROUND_ROBIN = new PartitionFunctionHandle(Optional.empty(), new SystemPartitionFunctionHandle(roundRobin));
    public static final PartitionFunctionHandle SINGLE = new PartitionFunctionHandle(Optional.empty(), new SystemPartitionFunctionHandle(single));

    public enum SystemPartitionFunctionId
    {
        hash, roundRobin, single
    }

    private final SystemPartitionFunctionId function;

    @JsonCreator
    public SystemPartitionFunctionHandle(@JsonProperty("function") SystemPartitionFunctionId function)
    {
        this.function = requireNonNull(function, "name is null");
    }

    @JsonProperty
    public SystemPartitionFunctionId getFunction()
    {
        return function;
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
        SystemPartitionFunctionHandle that = (SystemPartitionFunctionHandle) o;
        return function == that.function;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function);
    }

    @Override
    public String toString()
    {
        return function.toString();
    }
}
