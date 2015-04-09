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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.routine.AnalyzedFunction;
import com.facebook.presto.sql.routine.FunctionAnalyzer;
import com.facebook.presto.sql.routine.RoutineCompiler;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Throwables;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.nCopies;

public class CompilingFunctionDecoder
        implements FunctionDecoder
{
    private final SqlParser sqlParser;
    private final TypeManager typeManager;

    @Inject
    public CompilingFunctionDecoder(SqlParser sqlParser, TypeManager typeManager)
    {
        this.sqlParser = sqlParser;
        this.typeManager = typeManager;
    }

    @Override
    public FunctionInfo decode(FunctionRegistry functionRegistry, String value)
    {
        Statement statement = sqlParser.createStatement(value);
        checkArgument(statement instanceof CreateFunction, "statement is not an instance of CreateFunction");
        CreateFunction createFunction = (CreateFunction) statement;

        FunctionAnalyzer analyzer = new FunctionAnalyzer(typeManager, functionRegistry);
        AnalyzedFunction result = analyzer.analyze(createFunction);

        RoutineCompiler compiler = new RoutineCompiler(functionRegistry);
        Class<?> clazz = compiler.compile(result.getRoutine());
        MethodHandle methodHandle = getRunMethod(clazz);

        return new FunctionInfo(result.getSignature(),
                null,
                false,
                methodHandle,
                createFunction.getRoutineCharacteristics().isDeterministic().orElse(true),
                true,
                nCopies(createFunction.getParameters().size(), false));
    }

    private static MethodHandle getRunMethod(Class<?> clazz)
    {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals("run")) {
                try {
                    return MethodHandles.lookup().unreflect(method);
                }
                catch (IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        throw new RuntimeException("cannot find run method");
    }
}
