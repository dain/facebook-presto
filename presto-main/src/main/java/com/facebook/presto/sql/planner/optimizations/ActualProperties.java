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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.sql.planner.DistributionHandle;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.COORDINATOR_ONLY;
import static com.facebook.presto.sql.planner.PlanFragment.PlanDistribution.FIXED;
import static com.facebook.presto.sql.planner.SystemDistributionHandle.createSystemDistribution;
import static com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.HASH;
import static com.facebook.presto.sql.planner.SystemPartitionFunctionHandle.SINGLE;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.NodeDistribution.coordinatorOnlyDistribution;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.NodeDistribution.singleDistribution;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.NodePartitioning.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.NodePartitioning.singlePartition;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

class ActualProperties
{
    private final Global global;
    private final List<LocalProperty<Symbol>> localProperties;
    private final Map<Symbol, Object> constants;

    private ActualProperties(
            Global global,
            List<? extends LocalProperty<Symbol>> localProperties,
            Map<Symbol, Object> constants)
    {
        requireNonNull(global, "globalProperties is null");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.global = global;

        // The constants field implies a ConstantProperty in localProperties (but not vice versa).
        // Let's make sure to include the constants into the local constant properties.
        Set<Symbol> localConstants = LocalProperties.extractLeadingConstants(localProperties);
        localProperties = LocalProperties.stripLeadingConstants(localProperties);

        Set<Symbol> updatedLocalConstants = ImmutableSet.<Symbol>builder()
                .addAll(localConstants)
                .addAll(constants.keySet())
                .build();

        List<LocalProperty<Symbol>> updatedLocalProperties = LocalProperties.normalizeAndPrune(ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(transform(updatedLocalConstants, ConstantProperty::new))
                .addAll(localProperties)
                .build());

        this.localProperties = ImmutableList.copyOf(updatedLocalProperties);
        this.constants = ImmutableMap.copyOf(constants);
    }

    public static ActualProperties distributed()
    {
        return builder()
                .global(Global.distributed())
                .build();
    }

    public static ActualProperties distributed(NodeDistribution nodeDistribution, Optional<NodePartitioning> partitioning)
    {
        return builder()
                .global(Global.distributed(nodeDistribution, partitioning))
                .build();
    }

    public static ActualProperties undistributed()
    {
        return builder()
                .global(Global.undistributed())
                .build();
    }

    public static ActualProperties partitioned(Set<Symbol> columns)
    {
        return builder()
                .global(Global.partitioned(columns))
                .build();
    }

    @Deprecated
    public static ActualProperties hashPartitioned(List<Symbol> columns)
    {
        return builder()
                .global(Global.hashDistributed(columns))
                .build();
    }

    public boolean isCoordinatorOnly()
    {
        return global.isCoordinatorOnly();
    }

    public boolean isDistributed()
    {
        return global.isDistributed();
    }

    public boolean isNullReplication()
    {
        checkState(global.getDistributionProperties().isPresent());
        return global.getDistributionProperties().get().isReplicateNulls();
    }

    public boolean isPartitionedOn(Collection<Symbol> columns)
    {
        return global.getPartitioningProperties().isPresent() && global.getPartitioningProperties().get().isPartitionedOn(columns, constants.keySet());
    }

    /**
     * @return true if all the data will effectively land in a single stream
     */
    public boolean isEffectivelySinglePartition()
    {
        return global.getPartitioningProperties().isPresent() && global.getPartitioningProperties().get().isEffectivelySinglePartition(constants.keySet());
    }

    /**
     * @return true if repartitioning on the keys will yield some difference
     */
    public boolean isRepartitionEffective(Collection<Symbol> keys)
    {
        return !global.getPartitioningProperties().isPresent() || global.getPartitioningProperties().get().isRepartitionEffective(keys, constants.keySet());
    }

    public ActualProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        Map<Symbol, Object> translatedConstants = new HashMap<>();
        for (Map.Entry<Symbol, Object> entry : constants.entrySet()) {
            Optional<Symbol> translatedKey = translator.apply(entry.getKey());
            if (translatedKey.isPresent()) {
                translatedConstants.put(translatedKey.get(), entry.getValue());
            }
        }
        return builder()
                .global(global.translate(translator))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translatedConstants)
                .build();
    }

    public boolean isDistributedOn(Iterable<Symbol> columns)
    {
        return global.getDistributionProperties().isPresent() && global.getDistributionProperties().get().isDistributedOn(columns);
    }

    public boolean isDistributedOn(DistributionHandle distributionHandle, PartitionFunctionBinding partitionFunction)
    {
        return global.getDistributionProperties().isPresent() && global.getDistributionProperties().get().isDistributedOn(distributionHandle, partitionFunction);
    }

    @Deprecated
    public boolean isHashPartitionedOn(List<Symbol> columns)
    {
        return isDistributedOn(createSystemDistribution(FIXED), new PartitionFunctionBinding(HASH, columns));
    }

    public Optional<DistributionHandle> getDistributionHandle()
    {
        return global.getDistributionProperties()
                .map(NodeDistribution::getDistributionHandle);
    }

    public Optional<PartitionFunctionBinding> getPartitionFunction()
    {
        return global.getDistributionProperties()
                .map(NodeDistribution::getPartitionFunction);
    }

    public Map<Symbol, Object> getConstants()
    {
        return constants;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(ActualProperties properties)
    {
        return new Builder(properties.global, properties.localProperties, properties.constants);
    }

    public static class Builder
    {
        private Global global;
        private List<LocalProperty<Symbol>> localProperties;
        private Map<Symbol, Object> constants;

        public Builder(Global global, List<LocalProperty<Symbol>> localProperties, Map<Symbol, Object> constants)
        {
            this.global = global;
            this.localProperties = localProperties;
            this.constants = constants;
        }

        public Builder()
        {
            this.global = null;
            this.localProperties = ImmutableList.of();
            this.constants = ImmutableMap.of();
        }

        public Builder global(Global global)
        {
            this.global = global;
            return this;
        }

        public Builder global(ActualProperties other)
        {
            this.global = other.global;
            return this;
        }

        public Builder local(List<? extends LocalProperty<Symbol>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public Builder local(ActualProperties other)
        {
            this.localProperties = ImmutableList.copyOf(other.localProperties);
            return this;
        }

        public Builder constants(Map<Symbol, Object> constants)
        {
            this.constants = ImmutableMap.copyOf(constants);
            return this;
        }

        public Builder constants(ActualProperties other)
        {
            this.constants = ImmutableMap.copyOf(other.constants);
            return this;
        }

        public ActualProperties build()
        {
            return new ActualProperties(global, localProperties, constants);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(global, localProperties, constants.keySet());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final ActualProperties other = (ActualProperties) obj;
        return Objects.equals(this.global, other.global)
                && Objects.equals(this.localProperties, other.localProperties)
                && Objects.equals(this.constants.keySet(), other.constants.keySet());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("globalProperties", global)
                .add("localProperties", localProperties)
                .add("constants", constants)
                .toString();
    }

    @Immutable
    public static final class Global
    {
        // Description of the distribution of the data across nodes
        private final Optional<NodeDistribution> distributionProperties; // if missing => distributed with some unknown scheme
        // Description of the data in a single stream (split)
        private final Optional<NodePartitioning> partitioningProperties; // if missing => partitioned with some unknown scheme

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single stream. However, this can still be a distributed plan in that the plan
        // will be distributed to multiple servers, but only one server will get all the data.

        private Global(Optional<NodeDistribution> distributionProperties, Optional<NodePartitioning> partitioningProperties)
        {
            this.distributionProperties = requireNonNull(distributionProperties, "distributionProperties is null");
            this.partitioningProperties = requireNonNull(partitioningProperties, "partitioningProperties is null");
        }

        public static Global coordinatorOnly()
        {
            return new Global(Optional.of(coordinatorOnlyDistribution()), Optional.of(singlePartition()));
        }

        public static Global undistributed()
        {
            return new Global(Optional.of(singleDistribution()), Optional.of(singlePartition()));
        }

        public static Global distributed()
        {
            return new Global(Optional.empty(), Optional.empty());
        }

        public static Global distributed(NodeDistribution nodeDistribution, Optional<NodePartitioning> partitioning)
        {
            return new Global(Optional.of(nodeDistribution), partitioning);
        }

        public static Global partitioned(Set<Symbol> columns)
        {
            return new Global(Optional.empty(), Optional.of(partitionedOn(columns)));
        }

        public static Global partitioned(NodePartitioning nodePartitioning)
        {
            return new Global(Optional.empty(), Optional.of(nodePartitioning));
        }

        public static Global hashDistributed(List<Symbol> columns)
        {
            return new Global(Optional.of(NodeDistribution.hashDistributed(columns)), Optional.of(partitionedOn(ImmutableSet.copyOf(columns))));
        }

        public boolean isDistributed()
        {
            return !distributionProperties.isPresent() || distributionProperties.get().isDistributed();
        }

        public boolean isCoordinatorOnly()
        {
            return distributionProperties.isPresent() && distributionProperties.get().isCoordinatorOnly();
        }

        public Optional<NodePartitioning> getPartitioningProperties()
        {
            return partitioningProperties;
        }

        public Optional<NodeDistribution> getDistributionProperties()
        {
            return distributionProperties;
        }

        public Global translate(Function<Symbol, Optional<Symbol>> translator)
        {
            return new Global(
                    distributionProperties.flatMap(distribution -> distribution.translate(translator)),
                    partitioningProperties.flatMap(partitioning -> partitioning.translate(translator)));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distributionProperties, partitioningProperties);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final Global other = (Global) obj;
            return Objects.equals(this.distributionProperties, other.distributionProperties) &&
                    Objects.equals(this.partitioningProperties, other.partitioningProperties);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("distributionProperties", distributionProperties)
                    .add("partitioningProperties", partitioningProperties)
                    .toString();
        }
    }

    @Immutable
    public static final class NodePartitioning
    {
        private final Set<Symbol> partitioningColumns;

        private NodePartitioning(Iterable<Symbol> partitioningColumns)
        {
            this.partitioningColumns = ImmutableSet.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }

        public static NodePartitioning partitionedOn(Iterable<Symbol> columns)
        {
            return new NodePartitioning(columns);
        }

        public static NodePartitioning singlePartition()
        {
            return partitionedOn(ImmutableSet.of());
        }

        public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants)
        {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return partitioningColumns.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .allMatch(columns::contains);
        }

        public boolean isEffectivelySinglePartition(Set<Symbol> knownConstants)
        {
            return isPartitionedOn(ImmutableSet.of(), knownConstants);
        }

        public boolean isRepartitionEffective(Collection<Symbol> keys, Set<Symbol> knownConstants)
        {
            Set<Symbol> keysWithoutConstants = keys.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .collect(toImmutableSet());
            return !partitioningColumns.stream()
                    .filter(symbol -> !knownConstants.contains(symbol))
                    .collect(toImmutableSet())
                    .equals(keysWithoutConstants);
        }

        public Optional<NodePartitioning> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableSet.Builder<Symbol> newPartitioningColumns = ImmutableSet.builder();
            for (Symbol partitioningColumn : partitioningColumns) {
                Optional<Symbol> translated = translator.apply(partitioningColumn);
                if (!translated.isPresent()) {
                    return Optional.empty();
                }
                newPartitioningColumns.add(translated.get());
            }

            return Optional.of(new NodePartitioning(newPartitioningColumns.build()));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningColumns);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NodePartitioning other = (NodePartitioning) obj;
            return Objects.equals(this.partitioningColumns, other.partitioningColumns);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("partitioningColumns", partitioningColumns)
                    .toString();
        }
    }

    @Immutable
    public static final class NodeDistribution
    {
        private final DistributionHandle distributionHandle;
        private final PartitionFunctionBinding partitionFunction;

        private NodeDistribution(DistributionHandle distributionHandle, PartitionFunctionBinding partitionFunction)
        {
            this.distributionHandle = requireNonNull(distributionHandle, "distributionHandle is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        }

        public static NodeDistribution distributedOn(DistributionHandle distributionHandle, PartitionFunctionBinding partitionFunction)
        {
            return new NodeDistribution(distributionHandle, partitionFunction);
        }

        public static NodeDistribution coordinatorOnlyDistribution()
        {
            return new NodeDistribution(createSystemDistribution(COORDINATOR_ONLY), new PartitionFunctionBinding(SINGLE, ImmutableList.of()));
        }

        public static NodeDistribution singleDistribution()
        {
            return new NodeDistribution(createSystemDistribution(PlanDistribution.SINGLE), new PartitionFunctionBinding(SINGLE, ImmutableList.of()));
        }

        @Deprecated
        public static NodeDistribution hashDistributed(List<Symbol> columns)
        {
            return new NodeDistribution(createSystemDistribution(FIXED), new PartitionFunctionBinding(HASH, columns));
        }

        public boolean isReplicateNulls()
        {
            return partitionFunction.isReplicateNulls();
        }

        public DistributionHandle getDistributionHandle()
        {
            return distributionHandle;
        }

        public PartitionFunctionBinding getPartitionFunction()
        {
            return partitionFunction;
        }

        public boolean isDistributed()
        {
            return !partitionFunction.getPartitioningColumns().isEmpty();
        }

        public boolean isDistributedOn(Iterable<Symbol> columns)
        {
            return ImmutableSet.copyOf(columns).containsAll(partitionFunction.getPartitioningColumns());
        }

        public boolean isDistributedOn(DistributionHandle distributionHandle, PartitionFunctionBinding partitionFunction)
        {
            requireNonNull(distributionHandle, "distributionHandle is null");
            requireNonNull(partitionFunction, "partitionFunction is null");
            return  this.distributionHandle.equals(distributionHandle) &&
                    this.partitionFunction.getFunctionHandle().equals(partitionFunction.getFunctionHandle()) &&
                    this.partitionFunction.getPartitioningColumns().equals(partitionFunction.getPartitioningColumns()) &&
                    this.partitionFunction.isReplicateNulls() == partitionFunction.isReplicateNulls();
        }

        public boolean isCoordinatorOnly()
        {
            return isDistributedOn(createSystemDistribution(FIXED), new PartitionFunctionBinding(SINGLE, ImmutableList.of()));
        }

        public Optional<NodeDistribution> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableList.Builder<Symbol> newParameters = ImmutableList.builder();
            for (Symbol partitioningColumn : partitionFunction.getPartitioningColumns()) {
                Optional<Symbol> translated = translator.apply(partitioningColumn);
                if (!translated.isPresent()) {
                    // there is no symbol for this parameter so we can't
                    // say anything about this distribution
                    return Optional.empty();
                }
                newParameters.add(translated.get());
            }

            PartitionFunctionBinding newPartitionFunction = new PartitionFunctionBinding(
                    partitionFunction.getFunctionHandle(),
                    newParameters.build(),
                    partitionFunction.getHashColumn()
                            .flatMap(translator::apply),
                    partitionFunction.isReplicateNulls(),
                    partitionFunction.getBucketToPartition());

            return Optional.of(new NodeDistribution(distributionHandle, newPartitionFunction));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distributionHandle, partitionFunction);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NodeDistribution other = (NodeDistribution) obj;
            return Objects.equals(this.distributionHandle, other.distributionHandle)
                    && Objects.equals(this.partitionFunction, other.partitionFunction);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("distribution", distributionHandle)
                    .add("partitionFunction", partitionFunction)
                    .toString();
        }
    }
}
