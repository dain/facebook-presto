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

import com.facebook.presto.spi.TableFunction;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public abstract class TableFunctionDescriptor
{
    private final List<Parameter> parameters;

    public TableFunctionDescriptor(List<Parameter> parameters)
    {
        this.parameters = ImmutableList.copyOf(parameters);
    }

    public List<Parameter> getParameters()
    {
        return parameters;
    }

    public abstract TableFunction specialize(Map<String, Object> arguments);

    public static class Parameter
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

    public enum ExtendedType
    {
        DESCRIPTOR, TABLE
    }
}
