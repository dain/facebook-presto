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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Signature;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Map.Entry;

public interface FunctionDecoder
{
    String FUNCTION_SESSION_PREFIX = "$PRESTO_FUNCTION$$";

    // This method will be called often so implementation must cache
    FunctionInfo decode(String value);

    default Map<Signature, FunctionInfo> loadFunctions(Session session)
    {
        ImmutableMap.Builder<Signature, FunctionInfo> functions = ImmutableMap.builder();
        for (Entry<String, String> entry : session.getSystemProperties().entrySet()) {
            if (entry.getKey().startsWith(FUNCTION_SESSION_PREFIX)) {
                FunctionInfo functionInfo = decode(entry.getValue());
                functions.put(functionInfo.getSignature(), functionInfo);
            }
        }
        return functions.build();
    }
}
