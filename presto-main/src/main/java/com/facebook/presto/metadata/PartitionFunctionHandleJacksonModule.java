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

import com.facebook.presto.spi.ConnectorPartitionFunctionHandle;
import com.facebook.presto.sql.planner.SystemPartitionFunctionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PartitionFunctionHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorPartitionFunctionHandle>
{
    private static final String SYSTEM_HANDLE_ID = "$system";

    @Inject
    public PartitionFunctionHandleJacksonModule(HandleResolver handleResolver)
    {
        super(ConnectorPartitionFunctionHandle.class, "type", new PartitionFunctionHandleJsonTypeIdResolver(handleResolver));
    }

    private static class PartitionFunctionHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<ConnectorPartitionFunctionHandle>
    {
        private final HandleResolver handleResolver;

        private PartitionFunctionHandleJsonTypeIdResolver(HandleResolver handleResolver)
        {
            this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(ConnectorPartitionFunctionHandle functionHandle)
        {
            if (functionHandle instanceof SystemPartitionFunctionHandle) {
                return SYSTEM_HANDLE_ID;
            }
            return handleResolver.getId(functionHandle);
        }

        @Override
        public Class<? extends ConnectorPartitionFunctionHandle> getType(String id)
        {
            if (SYSTEM_HANDLE_ID.equals(id)) {
                return SystemPartitionFunctionHandle.class;
            }
            return handleResolver.getPartitionFunctionHandle(id);
        }
    }
}
