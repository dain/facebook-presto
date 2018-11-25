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
package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class RemoteDispatchQueryFactory
        implements DispatchQueryFactory
{
    private final TransactionManager transactionManager;
    private final QueryMonitor queryMonitor;
    private final LocationFactory locationFactory;
    private final CoordinatorSelector coordinatorSelector;
    private final QueryDispatcher<?> queryDispatcher;

    @Inject
    public RemoteDispatchQueryFactory(
            TransactionManager transactionManager,
            QueryMonitor queryMonitor,
            LocationFactory locationFactory,
            CoordinatorSelector coordinatorSelector,
            QueryDispatcher<?> queryDispatcher)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.coordinatorSelector = requireNonNull(coordinatorSelector, "coordinatorSelector is null");
        this.queryDispatcher = requireNonNull(queryDispatcher, "queryDispatcher is null");
    }

    @Override
    public DispatchQuery createDispatchQuery(
            Session session,
            String query,
            PreparedQuery preparedQuery,
            String slug,
            ResourceGroupId resourceGroupId,
            Executor queryExecutor)
    {
        return new RemoteDispatchQuery<>(
                session,
                slug,
                query,
                preparedQuery,
                locationFactory.createQueryLocation(session.getQueryId()),
                resourceGroupId,
                coordinatorSelector,
                queryDispatcher,
                queryMonitor,
                transactionManager,
                queryExecutor);
    }
}
