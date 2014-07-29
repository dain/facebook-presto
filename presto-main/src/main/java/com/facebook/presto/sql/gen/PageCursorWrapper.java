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
package com.facebook.presto.sql.gen;

import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public class PageCursorWrapper
    implements RecordCursor
{
    private final Page page;
    private int position = -1;

    public PageCursorWrapper(Page page)
    {
        this.page = page;
    }

    @Override
    public long getTotalBytes()
    {
        return page.getDataSize().toBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean advanceNextPosition()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isNull(int field)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
