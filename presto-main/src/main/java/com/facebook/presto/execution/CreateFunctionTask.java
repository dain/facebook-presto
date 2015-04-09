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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.routine.AnalyzedFunction;
import com.facebook.presto.sql.routine.FunctionAnalyzer;
import com.facebook.presto.sql.tree.CreateFunction;

import static com.facebook.presto.execution.FunctionUtils.encodeFunctionSessionProperty;
import static com.facebook.presto.execution.FunctionUtils.getFunctionSessionPropertyName;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.sql.SqlFormatter.formatSql;

public class CreateFunctionTask
        implements DataDefinitionTask<CreateFunction>
{
    @Override
    public String getName()
    {
        return "CREATE FUNCTION";
    }

    @Override
    public void execute(CreateFunction statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        FunctionAnalyzer analyzer = new FunctionAnalyzer(metadata.getTypeManager(), metadata.getFunctionRegistry(session));
        AnalyzedFunction result = analyzer.analyze(statement);

        String sessionPropertyName = getFunctionSessionPropertyName(result.getSignature());
        if (!statement.isReplace() && session.getSystemProperties().containsKey(sessionPropertyName)) {
            throw new PrestoException(ALREADY_EXISTS, "Function \"" + statement.getName() + "\" already exists");
        }

        stateMachine.addSetSessionProperties(sessionPropertyName, encodeFunctionSessionProperty(formatSql(statement)));
    }
}
