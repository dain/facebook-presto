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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * An implementation of the StreamSummary algorithm described in <i>Efficient Computation of
 * Frequent and Top-k Elements in Data Streams</i> by Metwally, Agrawal and Abbadi.
 * <p>
 * This datstructure as outlined in the research paper is a O(1) insert for keeping track of top k frequent items in an
 * possibly infinite stream. The top k elements can be then retrieved in O(k)
 * <p>
 * Analysis:
 * <ul>
 * <li>Smallest counter value, min, is at most epsilon * N , where N is items seen in the stream</li>
 * <li>True count of an uncounted item is between 0 and min</li>
 * <li>Any item whose true true count is greater than epsilon * N is stored</li>
 * </ul>
 *
 * @see <a href="http://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop
 * -kElementsInDataStreams.pdf">research paper</a>
 */
public class StreamSummary<T>
{
    private final int capacity;
    private final Map<T, Counter<T>> cache = new HashMap<>();

    private Bucket<T> head;
    private Bucket<T> tail;

    private long count;

    /**
     * Constructor - initializes the SpaceSaving algorithm data structure with an expected error (epsilon).
     * The size of the data structure is inversely proportional to the epsilon (1/epsilon).
     * i.e. epsilon of 0.0001 will need 10000 counters
     * Any element with (frequency - error) > epsilon * N is guaranteed to be in the Stream-Summary
     *
     * @param epsilon The acceptable error percentage (0-1]
     */
    public StreamSummary(double epsilon)
    {
        checkArgument(epsilon > 0 && epsilon <= 1, "epsilon must be between 0 exclusively and 1 inclusively");
        this.capacity = (int) Math.ceil(1.0 / epsilon);

        Bucket<T> zeroBucket = new Bucket<>(0);
        for (int i = 0; i < capacity; i++) {
            zeroBucket.getChildren().add(new Counter<>(zeroBucket));
        }

        head = zeroBucket;
        tail = zeroBucket;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public void offer(T item)
    {
        offer(item, 1);
    }

    public void offer(T item, long increment)
    {
        requireNonNull(item, "item is null");

        count++;

        Counter<T> existingElement = cache.get(item);
        if (existingElement != null) {
            incrementCounter(existingElement);
        }
        else {
            // get the item with the minimum count
            Counter<T> minCounter = tail.getChildren().getFirst();

            // if the minimum counter contains an item, unlink the current item and update the error
            if (minCounter.getItem() != null) {
                cache.remove(minCounter.getItem());
                minCounter.setError(minCounter.getCount());
            }

            // place the new item in the counter
            cache.put(item, minCounter);
            minCounter.setItem(item);
            incrementCounter(minCounter);
        }

        verify(cache.size() <= capacity);
    }

    /**
     * Returns the number of items seen in the stream so far.
     * Any element with (frequency - error) > epsilon * count is guaranteed to be in the Stream-Summary.
     *
     * @return the number of items seen.
     */
    public long getCount()
    {
        return count;
    }

    public List<TopElement<T>> getTopElements(int number)
    {
        List<TopElement<T>> output = new ArrayList<>(number);

        Bucket<T> current = head;
        while (current != null && output.size() < number) {
            current.getChildren().stream()
                    .filter(counter -> counter.getItem() != null)
                    .map(counter -> new TopElement<>(counter.getItem(), counter.getCount(), counter.getError()))
                    .limit(number - output.size())
                    .forEach(output::add);
            current = current.getNext();
        }
        return ImmutableList.copyOf(output);
    }

    private void incrementCounter(Counter<T> counter)
    {
        Bucket<T> bucket = counter.getBucket();
        Bucket<T> bucketNext = bucket.getPrev();

        counter.increment();

        // if the existing bucket only contains this element and .....................
        if (bucket.getChildren().size() == 1 && (bucketNext == null || bucketNext.getValue() != counter.getCount())) {
            bucket.incrementValue();
            verify(counter.getCount() == bucket.getValue());
            return;
        }

        // remove the item from the current bucket
        bucket.getChildren().remove(counter);

        // increment count, and add item to bucket for the new count
        if (bucketNext != null && counter.getCount() == bucketNext.getValue()) {
            bucketNext.getChildren().addLast(counter);
            counter.setBucket(bucketNext);
        }
        else {
            Bucket<T> bucketNew = new Bucket<>(counter.getCount());
            bucketNew.getChildren().addLast(counter);
            insertBefore(bucket, bucketNew);
            counter.setBucket(bucketNew);
        }

        // if the bucket is now empty, remove it
        if (bucket.getChildren().isEmpty()) {
            remove(bucket);
        }
    }

    private void insertBefore(Bucket<T> existingBucket, Bucket<T> newBucket)
    {
        requireNonNull(existingBucket, "existingBucket is null");
        requireNonNull(newBucket, "newBucket is null");

        existingBucket.insertBefore(newBucket);
        if (head == existingBucket) {
            head = newBucket;
        }
    }

    private void remove(Bucket<T> bucket)
    {
        if (head == bucket) {
            head = bucket.getNext();
            verify(head != null, "Can not remove last bucket");
        }
        if (tail == bucket) {
            tail = bucket.getPrev();
            verify(tail != null, "Can not remove last bucket");
        }
        bucket.remove();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("count", count)
                .toString();
    }

    public static class TopElement<T>
    {
        private final T item;
        private final long count;
        private final long error;

        public TopElement(T item, long count, long error)
        {
            this.item = requireNonNull(item, "item is null");
            checkArgument(count >= 0, "count is negative");
            this.count = count;
            checkArgument(error >= 0, "error is negative");
            this.error = error;
        }

        public T getItem()
        {
            return item;
        }

        public long getCount()
        {
            return count;
        }

        public long getError()
        {
            return error;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("item", item)
                    .add("count", count)
                    .add("error", error)
                    .toString();
        }
    }
}
