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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ScopedFunctionRegistry
        implements FunctionRegistry
{
    private final FunctionRegistry delegate;
    private final Map<Signature, FunctionInfo> localFunctions;

    public ScopedFunctionRegistry(FunctionRegistry delegate, Map<Signature, FunctionInfo> localFunctions)
    {
        this.delegate = delegate;
        this.localFunctions = localFunctions;
    }

    @Override
    public FunctionInfo getExactFunction(Signature signature)
    {
        // check if there is a global function
        FunctionInfo functionInfo = delegate.getExactFunction(signature);

        // look for a local scoped functions
        if (functionInfo == null) {
            functionInfo = localFunctions.get(signature);
        }
        return functionInfo;
    }

    @Override
    public FunctionInfo resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
    {
        // check if there is a global function
        PrestoException notFoundException;
        try {
            return delegate.resolveFunction(name, parameterTypes, approximate);
        }
        catch (PrestoException e) {
            notFoundException = e;
        }

        // search for a local scoped function
        for (Entry<Signature, FunctionInfo> entry : localFunctions.entrySet()) {
            Signature signature = entry.getKey();
            if (signature.getName().equals(name.getSuffix()) && signature.getArgumentTypes().equals(parameterTypes)) {
                return entry.getValue();
            }
        }

        throw notFoundException;
    }

    @Override
    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        return delegate.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public FunctionInfo getCoercion(Type fromType, Type toType)
    {
        return delegate.getCoercion(fromType, toType);
    }
}
