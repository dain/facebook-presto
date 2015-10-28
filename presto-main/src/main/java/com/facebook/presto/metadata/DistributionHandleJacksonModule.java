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

import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.facebook.presto.sql.planner.SystemDistributionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class DistributionHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorDistributionHandle>
{
    private static final String SYSTEM_HANDLE_ID = "$system";

    @Inject
    public DistributionHandleJacksonModule(HandleResolver handleResolver)
    {
        super(ConnectorDistributionHandle.class, "type", new DistributionHandleJsonTypeIdResolver(handleResolver));
    }

    private static class DistributionHandleJsonTypeIdResolver
            implements JsonTypeIdResolver<ConnectorDistributionHandle>
    {
        private final HandleResolver handleResolver;

        private DistributionHandleJsonTypeIdResolver(HandleResolver handleResolver)
        {
            this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        }

        @Override
        public String getId(ConnectorDistributionHandle distributionHandle)
        {
            if (distributionHandle instanceof SystemDistributionHandle) {
                return SYSTEM_HANDLE_ID;
            }
            return handleResolver.getId(distributionHandle);
        }

        @Override
        public Class<? extends ConnectorDistributionHandle> getType(String id)
        {
            if (SYSTEM_HANDLE_ID.equals(id)) {
                return SystemDistributionHandle.class;
            }
            return handleResolver.getDistributionHandleClass(id);
        }
    }
}
