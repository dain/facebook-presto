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

public class TableFunction
        extends Relation
{
    private final RoutineInvocation call;

    public TableFunction(Optional<NodeLocation> location, RoutineInvocation call)
    {
        super(location);
        this.call = call;
    }

    public TableFunction(NodeLocation location, RoutineInvocation call)
    {
        this(Optional.of(location), call);
    }

    public RoutineInvocation getCall()
    {
        return call;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunction(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(call);
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
        TableFunction that = (TableFunction) o;
        return Objects.equals(call, that.call);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(call);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("call", call)
                .toString();
    }
}
