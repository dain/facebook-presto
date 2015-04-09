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
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropFunction;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;

public class DropFunctionTask
        implements DataDefinitionTask<DropFunction>
{
    @Override
    public String getName()
    {
        return "DROP FUNCTION";
    }

    @Override
    public void execute(DropFunction statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        Map<String, String> sessionFunctions = FunctionUtils.getSignatureToProperty(session);

        String functionName = statement.getName().toString();

        String sessionProperty = sessionFunctions.get(functionName);
        if (sessionProperty == null) {
            String signaturePrefix = functionName + '(';
            Set<String> matchingFunctions = new LinkedHashSet<>();
            for (Entry<String, String> entry : sessionFunctions.entrySet()) {
                if (entry.getKey().startsWith(signaturePrefix)) {
                    matchingFunctions.add(entry.getKey());
                    sessionProperty = entry.getValue();
                }
            }
            if (matchingFunctions.size() > 1) {
                throw new SemanticException(AMBIGUOUS_ATTRIBUTE, statement, "Ambiguous function name. Matching functions are %s", matchingFunctions);
            }
            if (matchingFunctions.isEmpty()) {
                if (statement.isIfExists()) {
                    return;
                }
                throw new PrestoException(NOT_FOUND, "Function \"" + statement.getName() + "\" does not exist exists");
            }
        }

        stateMachine.addResetSessionProperties(sessionProperty);
    }
}
