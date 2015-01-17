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
package com.facebook.presto.sql.routine;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class SqlBlock
        implements SqlStatement
{
    private final Optional<SqlLabel> label;
    private final List<SqlVariable> variables;
    private final List<SqlStatement> statements;

    public SqlBlock(List<SqlVariable> variables, List<SqlStatement> statements)
    {
        this(Optional.empty(), variables, statements);
    }

    public SqlBlock(Optional<SqlLabel> label, List<SqlVariable> variables, List<SqlStatement> statements)
    {
        this.label = checkNotNull(label, "label is null");
        this.variables = ImmutableList.copyOf(checkNotNull(variables, "variables is null"));
        this.statements = ImmutableList.copyOf(checkNotNull(statements, "statements is null"));
    }

    public Optional<SqlLabel> getLabel()
    {
        return label;
    }

    public List<SqlVariable> getVariables()
    {
        return variables;
    }

    public List<SqlStatement> getStatements()
    {
        return statements;
    }

    @Override
    public <C, R> R accept(SqlNodeVisitor<C, R> visitor, C context)
    {
        return visitor.visitBlock(this, context);
    }
}
