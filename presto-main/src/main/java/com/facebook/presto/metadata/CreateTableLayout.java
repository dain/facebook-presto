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

import com.facebook.presto.spi.ConnectorCreateTableLayout;
import com.facebook.presto.sql.planner.DistributionHandle;
import com.facebook.presto.sql.planner.PartitionFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateTableLayout
{
    private final String connectorId;
    private final ConnectorCreateTableLayout layout;

    @JsonCreator
    public CreateTableLayout(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("layout") ConnectorCreateTableLayout layout)
    {
        this.connectorId = connectorId;
        this.layout = layout;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorCreateTableLayout getLayout()
    {
        return layout;
    }

    public DistributionHandle getDistribution()
    {
        return new DistributionHandle(Optional.of(connectorId), layout.getDistribution());
    }

    public List<String> getPartitionColumns()
    {
        return layout.getPartitionColumns();
    }

    public PartitionFunctionHandle getPartitionFunction()
    {
        return new PartitionFunctionHandle(Optional.of(connectorId), layout.getPartitionFunction());
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

        CreateTableLayout that = (CreateTableLayout) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, layout);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("layout", layout)
                .toString();
    }
}
