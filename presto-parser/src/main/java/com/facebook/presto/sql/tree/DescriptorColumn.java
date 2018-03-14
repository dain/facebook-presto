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

public class DescriptorColumn
        extends Node
{
    private final Identifier name;
    private final Optional<String> type;

    public DescriptorColumn(Optional<NodeLocation> location, Identifier column, Optional<String> type)
    {
        super(location);
        this.name = column;
        this.type = type;
    }

    public DescriptorColumn(NodeLocation location, Identifier column, Optional<String> type)
    {
        this(Optional.of(location), column, type);
    }

    public Identifier getName()
    {
        return name;
    }

    public Optional<String> getType()
    {
        return type;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDescriptorColumn(this, context);
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
        DescriptorColumn that = (DescriptorColumn) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", name)
                .add("type", type)
                .toString();
    }
}
