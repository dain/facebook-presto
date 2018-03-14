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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.TableFunction;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.function.PolymorphicTableFunction.InputTableSemantics.ROW;

public interface PolymorphicTableFunction
{
    enum InputTableSemantics
    {
        SET, ROW
    }

    String getName();

    List<Parameter> getParameters();

    default boolean isDeterministic()
    {
        return false;
    }

    default InputTableSemantics getInputTableSemantics()
    {
        return ROW;
    }

    default boolean isWithPassThrough()
    {
        return false;
    }

    TableFunction specialize(Map<String, Object> arguments);

    TableFunctionImplementation getInstance(byte[] handle);

    class ColumnDescriptor
    {
        private final String name;
        private final Optional<TypeSignature> type;

        public ColumnDescriptor(String name, Optional<TypeSignature> type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public Optional<TypeSignature> getType()
        {
            return type;
        }
    }

    class Parameter
    {
        private final String name;
        private final Object type;  // a TypeSignature, or a value of ExtendedType

        public Parameter(String name, Object type)
        {
            this.name = name;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public Object getType()
        {
            return type;
        }
    }

    enum ExtendedType
    {
        DESCRIPTOR, TABLE
    }
}
