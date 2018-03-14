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
package com.facebook.presto.operator.ptf.spacesaving;

import java.util.ArrayDeque;
import java.util.Deque;

import static com.google.common.base.MoreObjects.toStringHelper;

class Bucket<T>
{
    private Bucket<T> next;
    private Bucket<T> prev;

    private long value;

    private final Deque<Counter<T>> children = new ArrayDeque<>();

    Bucket(long value)
    {
        this.value = value;
    }

    public Bucket<T> getNext()
    {
        return next;
    }

    public Bucket<T> getPrev()
    {
        return prev;
    }

    public long getValue()
    {
        return value;
    }

    public void incrementValue()
    {
        value++;
    }

    public Deque<Counter<T>> getChildren()
    {
        return children;
    }

    public void insertBefore(Bucket<T> newBucket)
    {
        newBucket.prev = this.prev;
        newBucket.next = this;
        if (this.prev != null) {
            this.prev.next = newBucket;
        }
        this.prev = newBucket;
    }

    public void remove()
    {
        if (prev != null) {
            prev.next = next;
        }
        if (next != null) {
            next.prev = prev;
        }

        next = null;
        prev = null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("children", children)
                .add("value", value)
                .toString();
    }
}
