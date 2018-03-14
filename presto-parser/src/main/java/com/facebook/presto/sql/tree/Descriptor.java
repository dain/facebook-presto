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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Descriptor
        extends Node
{
    private final List<DescriptorColumn> columns;

    public Descriptor(Optional<NodeLocation> location, List<DescriptorColumn> columns)
    {
        super(location);
        this.columns = ImmutableList.copyOf(columns);
    }

    public Descriptor(NodeLocation location, List<DescriptorColumn> columns)
    {
        this(Optional.of(location), columns);
    }

    public List<DescriptorColumn> getColumns()
    {
        return columns;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return columns;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDescriptor(this, context);
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
        Descriptor that = (Descriptor) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }
}
