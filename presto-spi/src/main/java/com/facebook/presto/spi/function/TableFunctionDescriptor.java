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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.ColumnMetadata;

import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class TableFunctionDescriptor
{
    private final byte[] handle;
    private final List<Integer> inputColumns;
    private final List<ColumnMetadata> outputColumns;

    public TableFunctionDescriptor(byte[] handle, List<Integer> inputColumns, List<ColumnMetadata> outputColumns)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.inputColumns = unmodifiableList(requireNonNull(inputColumns, "inputColumns is null"));
        this.outputColumns = unmodifiableList(requireNonNull(outputColumns, "outputColumns is null"));
    }

    public byte[] getHandle()
    {
        return handle;
    }

    public List<Integer> getInputColumns()
    {
        return inputColumns;
    }

    public List<ColumnMetadata> getOutputColumns()
    {
        return outputColumns;
    }
}
