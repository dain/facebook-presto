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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class TableFunctionCall
        extends PlanNode
{
    private final String name;
    private final byte[] handle;
    private final boolean singleNode;
    private final List<Symbol> outputs;
    private final List<Symbol> inputFields;
    private final PlanNode input;

    public TableFunctionCall(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("name") String name,
            @JsonProperty("handle") byte[] handle,
            @JsonProperty("singleNode") boolean singleNode,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("inputFields") List<Symbol> inputFields,
            @JsonProperty("input") PlanNode input)
    {
        super(id);

        checkArgument(input.getOutputSymbols().containsAll(inputFields), "Missing dependencies: have=%s vs need=%s", input.getOutputSymbols(), inputFields);

        this.name = name;
        this.handle = handle;
        this.singleNode = singleNode;
        this.outputs = outputs;
        this.inputFields = ImmutableList.copyOf(inputFields);
        this.input = input;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public byte[] getHandle()
    {
        return handle;
    }

    @JsonProperty
    public boolean isSingleNode()
    {
        return singleNode;
    }

    @JsonProperty
    public PlanNode getInput()
    {
        return input;
    }

    @JsonProperty
    public List<Symbol> getInputFields()
    {
        return inputFields;
    }

    @JsonProperty
    public List<Symbol> getOutputs()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TableFunctionCall(getId(), name, handle, singleNode, outputs, inputFields, Iterables.getOnlyElement(newChildren));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFunctionCall(this, context);
    }
}
