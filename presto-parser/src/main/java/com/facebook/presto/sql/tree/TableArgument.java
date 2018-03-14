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

public class TableArgument
        extends Node
{
    private final Node table;

    public TableArgument(Optional<NodeLocation> location, Node table)
    {
        super(location);
        this.table = table;
    }

    public TableArgument(NodeLocation location, Node table)
    {
        this(Optional.of(location), table);
    }

    public Node getTable()
    {
        return table;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableArgument(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(table);
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
        TableArgument that = (TableArgument) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .toString();
    }
}
