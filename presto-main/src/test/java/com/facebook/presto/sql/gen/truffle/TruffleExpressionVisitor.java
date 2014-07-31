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
package com.facebook.presto.sql.gen.truffle;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.AndNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.BooleanConstantNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.BooleanInputNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.DoubleConstantNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.DoubleInputNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.ExpressionNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.FunctionNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.LongConstantNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.LongInputNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.SliceConstantNode;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.SliceInputNode;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

public class TruffleExpressionVisitor
        implements RowExpressionVisitor<FrameDescriptor, ExpressionNode>
{
    private final FunctionRegistry registry;

    public TruffleExpressionVisitor(FunctionRegistry registry)
    {
        this.registry = registry;
    }

    @Override
    public ExpressionNode visitCall(CallExpression call, FrameDescriptor frameDescriptor)
    {
        Signature signature = call.getSignature();
        List<RowExpression> arguments = call.getArguments();
        ExpressionNode[] argumentNodes = new ExpressionNode[arguments.size()];
        for (int i = 0; i < argumentNodes.length; i++) {
            argumentNodes[i] = arguments.get(i).accept(this, frameDescriptor);
        }

        switch (call.getSignature().getName()) {
            case "AND":
                return new AndNode(argumentNodes[0], argumentNodes[1]);
        }

        FunctionInfo function = registry.getExactFunction(signature);
        if (function == null) {
            // TODO: temporary hack to deal with magic timestamp literal functions which don't have an "exact" form and need to be "resolved"
            function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
        }

        Preconditions.checkArgument(function != null, "Function %s not found", signature);


        return new FunctionNode(function.getMethodHandle(), argumentNodes);
    }

    @Override
    public ExpressionNode visitConstant(ConstantExpression constant, FrameDescriptor frameDescriptor)
    {
        Object value = constant.getValue();
        Class<?> javaType = constant.getType().getJavaType();

        if (value == null) {
            return new SliceConstantNode(null);
        }

        if (javaType == boolean.class) {
            return new BooleanConstantNode((Boolean) value);
        }
        if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
            return new LongConstantNode((Long) value);
        }
        if (javaType == float.class || javaType == double.class) {
            return new DoubleConstantNode((Double) value);
        }
        if (javaType == Slice.class) {
            if (value instanceof String) {
                value = Slices.utf8Slice((String) value);
            }
            return new SliceConstantNode((Slice) value);
        }
        throw new IllegalArgumentException("Unsupported type " + javaType);
    }

    @Override
    public ExpressionNode visitInputReference(InputReferenceExpression node, FrameDescriptor frameDescriptor)
    {
        int field = node.getField();
        Type type = node.getType();
        Class<?> javaType = node.getType().getJavaType();

        FrameSlot pageSlot = frameDescriptor.findOrAddFrameSlot("page", FrameSlotKind.Object);
        FrameSlot rowSlot = frameDescriptor.findOrAddFrameSlot("row", FrameSlotKind.Int);

        if (javaType == boolean.class) {
            return new BooleanInputNode(pageSlot, rowSlot, type, field);
        }
        if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
            return new LongInputNode(pageSlot, rowSlot, type, field);
        }
        if (javaType == float.class || javaType == double.class) {
            return new DoubleInputNode(pageSlot, rowSlot, type, field);
        }
        if (javaType == Slice.class) {
            return new SliceInputNode(pageSlot, rowSlot, type, field);
        }
        throw new IllegalArgumentException("Unsupported type " + javaType);
    }
}
