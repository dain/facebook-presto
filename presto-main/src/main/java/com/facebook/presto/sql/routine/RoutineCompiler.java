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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.WhileLoop;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ByteCodeExpressionVisitor;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.ByteCodeUtils.boxPrimitive;
import static com.facebook.presto.sql.gen.ByteCodeUtils.unboxPrimitive;
import static com.facebook.presto.sql.gen.ByteCodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Maps.toMap;
import static com.google.common.primitives.Primitives.wrap;

public final class RoutineCompiler
{
    private final FunctionRegistry functionRegistry;

    public RoutineCompiler(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = checkNotNull(functionRegistry, "functionRegistry is null");
    }

    public Class<?> compile(SqlRoutine routine)
    {
        List<Parameter> parameters = routine.getParameters().stream()
                .map(variable -> arg(name(variable), compilerType(variable.getType())))
                .collect(toImmutableList());

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("SqlRoutine"),
                type(Object.class));

        classDefinition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                "run",
                compilerType(routine.getReturnType()),
                parameters);

        Scope scope = method.getScope();

        scope.declareVariable(boolean.class, "wasNull");

        Map<SqlVariable, Variable> variables = toMap(VariableExtractor.extract(routine),
                variable -> getOrDeclareVariable(scope, compilerType(variable.getType()), name(variable)));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ByteCodeVisitor visitor = new ByteCodeVisitor(variables, callSiteBinder, functionRegistry);
        method.getBody().append(visitor.process(routine, scope));

        return defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader());
    }

    private static Variable getOrDeclareVariable(Scope scope, ParameterizedType type, String name)
    {
        if (scope.variableExists(name)) {
            return scope.getVariable(name);
        }
        return scope.declareVariable(type, name);
    }

    private static ParameterizedType compilerType(Type type)
    {
        return type(wrap(type.getJavaType()));
    }

    private static String name(SqlVariable variable)
    {
        return name(variable.getField());
    }

    private static String name(int field)
    {
        return "v" + field;
    }

    private static class ByteCodeVisitor
            implements SqlNodeVisitor<Scope, ByteCodeNode>
    {
        private final Map<SqlVariable, Variable> variables;
        private final CallSiteBinder callSiteBinder;
        private final Map<SqlLabel, LabelNode> continueLabels = new IdentityHashMap<>();
        private final Map<SqlLabel, LabelNode> breakLabels = new IdentityHashMap<>();
        private final FunctionRegistry functionRegistry;

        public ByteCodeVisitor(Map<SqlVariable, Variable> variables, CallSiteBinder callSiteBinder, FunctionRegistry functionRegistry)
        {
            this.variables = checkNotNull(variables, "variables is null");
            this.callSiteBinder = checkNotNull(callSiteBinder, "callSiteBinder is null");
            this.functionRegistry = checkNotNull(functionRegistry, "functionRegistry is null");
        }

        @Override
        public ByteCodeNode visitRoutine(SqlRoutine node, Scope scope)
        {
            return process(node.getBody(), scope);
        }

        @Override
        public ByteCodeNode visitSet(SqlSet node, Scope scope)
        {
            return new Block()
                    .append(compile(node.getValue(), scope))
                    .putVariable(variables.get(node.getTarget()));
        }

        @Override
        public ByteCodeNode visitBlock(SqlBlock node, Scope scope)
        {
            Block block = new Block();

            for (SqlVariable sqlVariable : node.getVariables()) {
                block.append(compile(sqlVariable.getDefaultValue(), scope))
                        .putVariable(variables.get(sqlVariable));
            }

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");
            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            for (SqlStatement statement : node.getStatements()) {
                block.append(process(statement, scope));
            }

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public ByteCodeNode visitReturn(SqlReturn node, Scope scope)
        {
            return new Block()
                    .append(compile(node.getValue(), scope))
                    .ret(wrap(node.getValue().getType().getJavaType()));
        }

        @Override
        public ByteCodeNode visitContinue(SqlContinue node, Scope scope)
        {
            LabelNode label = continueLabels.get(node.getTarget());
            verify(label != null, "continue target does not exist");
            return new Block()
                    .gotoLabel(label);
        }

        @Override
        public ByteCodeNode visitBreak(SqlBreak node, Scope scope)
        {
            LabelNode label = breakLabels.get(node.getTarget());
            verify(label != null, "break target does not exist");
            return new Block()
                    .gotoLabel(label);
        }

        @Override
        public ByteCodeNode visitIf(SqlIf node, Scope scope)
        {
            IfStatement ifStatement = new IfStatement()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .ifTrue(process(node.getIfTrue(), scope));

            if (node.getIfFalse().isPresent()) {
                ifStatement.ifFalse(process(node.getIfFalse().get(), scope));
            }

            return ifStatement;
        }

        @Override
        public ByteCodeNode visitCase(SqlCase node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteCodeNode visitSwitch(SqlSwitch node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteCodeNode visitWhile(SqlWhile node, Scope scope)
        {
            Block block = new Block();

            LabelNode continueLabel = new LabelNode("continue");
            LabelNode breakLabel = new LabelNode("break");

            if (node.getLabel().isPresent()) {
                continueLabels.put(node.getLabel().get(), continueLabel);
                breakLabels.put(node.getLabel().get(), breakLabel);
                block.visitLabel(continueLabel);
            }

            block.append(new WhileLoop()
                    .condition(compileBoolean(node.getCondition(), scope))
                    .body(process(node.getBody(), scope)));

            if (node.getLabel().isPresent()) {
                block.visitLabel(breakLabel);
            }

            return block;
        }

        @Override
        public ByteCodeNode visitRepeat(SqlRepeat node, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        private ByteCodeNode compile(RowExpression expression, Scope scope)
        {
            if (expression instanceof InputReferenceExpression) {
                InputReferenceExpression input = (InputReferenceExpression) expression;
                return scope.getVariable(name(input.getField()));
            }

            Type type = expression.getType();
            ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(callSiteBinder, FieldReferenceCompiler.INSTANCE, functionRegistry);
            Variable wasNull = scope.getVariable("wasNull");

            return new Block()
                    .comment("boolean wasNull = false;")
                    .putVariable(wasNull, type.getJavaType() == void.class)
                    .comment("expression: " + expression)
                    .append(expression.accept(visitor, scope))
                    .append(boxPrimitive(type.getJavaType()))
                    .comment("if (wasNull)")
                    .append(new IfStatement()
                            .condition(wasNull)
                            .ifTrue(new Block()
                                    .pop()
                                    .pushNull()));
        }

        private ByteCodeNode compileBoolean(RowExpression expression, Scope scope)
        {
            checkArgument(expression.getType().equals(BooleanType.BOOLEAN), "type must be boolean");

            LabelNode notNull = new LabelNode("notNull");
            LabelNode done = new LabelNode("done");

            return new Block()
                    .append(compile(expression, scope))
                    .comment("if value is null, return false, otherwise unbox")
                    .dup()
                    .ifNotNullGoto(notNull)
                    .pop()
                    .push(false)
                    .gotoLabel(done)
                    .visitLabel(notNull)
                    .append(unboxPrimitive(expression.getType().getJavaType()))
                    .visitLabel(done);
        }
    }

    private static class FieldReferenceCompiler
            implements RowExpressionVisitor<Scope, ByteCodeNode>
    {
        public static final FieldReferenceCompiler INSTANCE = new FieldReferenceCompiler();

        @Override
        public ByteCodeNode visitInputReference(InputReferenceExpression node, Scope scope)
        {
            Class<?> boxedType = wrap(node.getType().getJavaType());
            return new Block()
                    .append(scope.getVariable(name(node.getField())))
                    .append(unboxPrimitiveIfNecessary(scope, boxedType));
        }

        @Override
        public ByteCodeNode visitCall(CallExpression call, Scope scope)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteCodeNode visitConstant(ConstantExpression literal, Scope scope)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class VariableExtractor
            extends DefaultSqlNodeVisitor
    {
        private final List<SqlVariable> variables = new ArrayList<>();

        @Override
        public Void visitVariable(SqlVariable node, Void context)
        {
            variables.add(node);
            return null;
        }

        public static List<SqlVariable> extract(SqlNode node)
        {
            VariableExtractor extractor = new VariableExtractor();
            extractor.process(node, null);
            return extractor.variables;
        }
    }
}
