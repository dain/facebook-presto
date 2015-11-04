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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RaptorDistributionHandle
        implements ConnectorDistributionHandle
{
    private final String connectorId;
    private final long distributionId;

    @JsonCreator
    public RaptorDistributionHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("distributionId") long distributionId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.distributionId = distributionId;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public long getDistributionId()
    {
        return distributionId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        RaptorDistributionHandle that = (RaptorDistributionHandle) o;
        return distributionId == that.distributionId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(distributionId);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + distributionId;
    }
}
