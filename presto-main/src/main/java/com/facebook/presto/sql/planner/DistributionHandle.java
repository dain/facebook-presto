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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ConnectorDistributionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DistributionHandle
{
    private final Optional<String> connectorId;
    private final ConnectorDistributionHandle connectorHandle;

    @JsonCreator
    public DistributionHandle(
            @JsonProperty("connectorId") Optional<String> connectorId,
            @JsonProperty("connectorHandle") ConnectorDistributionHandle connectorHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    public Optional<String> getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorDistributionHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributionHandle that = (DistributionHandle) o;

        // Currently, custom distributions can not be equal since it will result
        // in plans containing multiple table scans, which is not supported.
        // TODO remove then when collocated plans are supported
        if (connectorId.isPresent() || that.connectorId.isPresent()) {
            return false;
        }

        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(connectorHandle, that.connectorHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, connectorHandle);
    }

    @Override
    public String toString()
    {
        if (connectorId.isPresent()) {
            return connectorId.get() + ":" + connectorHandle;
        }
        return connectorHandle.toString();
    }
}
