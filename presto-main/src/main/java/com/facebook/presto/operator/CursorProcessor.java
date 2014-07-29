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
package com.facebook.presto.operator;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.gen.FilterAndProject;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class CursorProcessor
    implements FilterAndProject
{
    private final FilterFunction filterFunction;
    private final List<? extends ProjectionFunction> projections;

    public CursorProcessor(FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
    {
        this.filterFunction = filterFunction;
        this.projections = ImmutableList.copyOf(projections);
    }

    @Override
    public int process(ConnectorSession session, Object input, int start, int end, PageBuilder pageBuilder)
    {
        RecordCursor cursor = (RecordCursor) input;

        int position = start;
        for (; position < end; position++) {
            if (pageBuilder.isFull()) {
                break;
            }

            if (!cursor.advanceNextPosition()) {
                break;
            }

            if (filterFunction.filter(cursor)) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < projections.size(); channel++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(channel).project(cursor, pageBuilder.getBlockBuilder(channel));
                }
            }
        }
        return position;
    }
}
