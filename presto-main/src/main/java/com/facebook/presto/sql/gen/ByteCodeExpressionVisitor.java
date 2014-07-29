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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodType;

import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;
import static com.facebook.presto.sql.relational.Signatures.TRY_CAST;

public class ByteCodeExpressionVisitor
        implements RowExpressionVisitor<CompilerContext, ByteCodeNode>
{
    private final CallSiteBinder callSiteBinder;
    private final ByteCodeNode getSessionByteCode;
    private final FunctionRegistry registry;

    public ByteCodeExpressionVisitor(
            CallSiteBinder callSiteBinder,
            ByteCodeNode getSessionByteCode,
            FunctionRegistry registry)
    {
        this.callSiteBinder = callSiteBinder;
        this.getSessionByteCode = getSessionByteCode;
        this.registry = registry;
    }

    @Override
    public ByteCodeNode visitCall(CallExpression call, final CompilerContext context)
    {
        ByteCodeGenerator generator;
        switch (call.getSignature().getName()) {
            // lazy evaluation
            case IF:
                generator = new IfCodeGenerator();
                break;
            case NULL_IF:
                generator = new NullIfCodeGenerator();
                break;
            case SWITCH:
                // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                generator = new SwitchCodeGenerator();
                break;
            // special-cased in function registry
            case CAST:
                generator = new CastCodeGenerator();
                break;
            case TRY_CAST:
                generator = new TryCastCodeGenerator();
                break;
            // functions that take null as input
            case IS_NULL:
                generator = new IsNullCodeGenerator();
                break;
            case "IS_DISTINCT_FROM":
                generator = new IsDistinctFromCodeGenerator();
                break;
            case COALESCE:
                generator = new CoalesceCodeGenerator();
                break;
            // functions that require varargs and/or complex types (e.g., lists)
            case IN:
                generator = new InCodeGenerator();
                break;
            // optimized implementations (shortcircuiting behavior)
            case "AND":
                generator = new AndCodeGenerator();
                break;
            case "OR":
                generator = new OrCodeGenerator();
                break;
            default:
                generator = new FunctionCallCodeGenerator();
        }

        ByteCodeGeneratorContext generatorContext = new ByteCodeGeneratorContext(
                this,
                context,
                callSiteBinder,
                getSessionByteCode,
                registry);

        return generator.generateExpression(call.getSignature(), generatorContext, call.getType(), call.getArguments());
    }

    @Override
    public ByteCodeNode visitConstant(ConstantExpression constant, CompilerContext context)
    {
        Object value = constant.getValue();
        Class<?> javaType = constant.getType().getJavaType();

        Block block = new Block(context);
        if (value == null) {
            return block.comment("constant null")
                    .putVariable("wasNull", true)
                    .pushJavaDefault(javaType);
        }

        // use LDC for primitives (boolean, short, int, long, float, double)
        block.comment("constant " + constant.getType().getName());
        if (javaType == boolean.class) {
            return block.append(loadBoolean((Boolean) value));
        }
        if (javaType == byte.class || javaType == short.class || javaType == int.class) {
            return block.append(loadInt(((Number) value).intValue()));
        }
        if (javaType == long.class) {
            return block.append(loadLong((Long) value));
        }
        if (javaType == float.class) {
            return block.append(loadFloat((Float) value));
        }
        if (javaType == double.class) {
            return block.append(loadDouble((Double) value));
        }
        if (javaType == String.class) {
            return block.append(loadString((String) value));
        }

        // bind constant object directly into the call-site using invoke dynamic
        Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

        return new Block(context)
                .setDescription("constant " + constant.getType())
                .comment(constant.toString())
                .append(loadConstant(context, binding));
    }

    @Override
    public ByteCodeNode visitInputReference(InputReferenceExpression node, CompilerContext context)
    {
        int field = node.getField();
        Type type = node.getType();

        Class<?> javaType = type.getJavaType();

        Block isNullCheck = new Block(context)
                .getVariable("input")
                .getVariable("position")
                .invokeDynamic("isNull", MethodType.methodType(boolean.class, Object.class, int.class), FieldReferenceBootstrap.BOOTSTRAP_METHOD, field, -1);

        Block isNull = new Block(context)
                .putVariable("wasNull", true)
                .pushJavaDefault(javaType);

        Binding typeConstant = callSiteBinder.bind(type, type.getClass());

        String getterName = "get" + Primitives.wrap(javaType).getSimpleName();
        Block isNotNull = new Block(context)
                .getVariable("input")
                .getVariable("position")
                .invokeDynamic(getterName, MethodType.methodType(javaType, Object.class, int.class), FieldReferenceBootstrap.BOOTSTRAP_METHOD, field, typeConstant.getBindingId());

        return new IfStatement(context, isNullCheck, isNull, isNotNull);
    }
}
