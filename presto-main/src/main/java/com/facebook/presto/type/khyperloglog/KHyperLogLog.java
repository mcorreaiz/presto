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

package com.facebook.presto.type.khyperloglog;

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

/**
 * For reference on KHyperLogLog, see "KHyperLogLog: Estimating Reidentifiability and
 * Joinability of Large Data at Scale" by Chia et al., 2019.
 */
public class KHyperLogLog
{
    private static final byte UNCOMPRESSED_FORMAT = 1;
    public static final int NUMBER_OF_BUCKETS = 512;
    public static final int DEFAULT_K = 2048;
    private static final long HASH_OUTPUT_HALF_RANGE = Long.MAX_VALUE;
    private static final int SIZE_OF_KHYPERLOGLOG = ClassLayout.parseClass(KHyperLogLog.class).instanceSize();
    private static final int SIZE_OF_RBTREEMAP = ClassLayout.parseClass(Long2ObjectRBTreeMap.class).instanceSize();

    private final Long2ObjectSortedMap<HyperLogLog> minhash;
    private final int K;
    private final int hllBuckets;

    public KHyperLogLog()
    {
        this(DEFAULT_K, NUMBER_OF_BUCKETS, new Long2ObjectRBTreeMap<>());
    }

    public KHyperLogLog(int K, int hllBuckets)
    {
        this(K, hllBuckets, new Long2ObjectRBTreeMap<>());
    }

    public KHyperLogLog(int K, int hllBuckets, Long2ObjectSortedMap<HyperLogLog> minhash)
    {
        this.K = K;
        this.hllBuckets = hllBuckets;
        this.minhash = requireNonNull(minhash, "minhash is null");
    }

    public static KHyperLogLog newInstance(Slice serialized)
    {
        // TODO: Review
        requireNonNull(serialized, "serialized is null");
        SliceInput input = serialized.getInput();
        checkArgument(input.readByte() == UNCOMPRESSED_FORMAT, "Unexpected version");

        int K = input.readInt();
        int hllBuckets = input.readInt();
        int minhashSize = input.readInt();

        Long2ObjectRBTreeMap<HyperLogLog> minhash = new Long2ObjectRBTreeMap<>();
        // The values are stored after the keys
        SliceInput valuesInput = serialized.getInput();
        valuesInput.setPosition(input.position() + minhashSize * SIZE_OF_LONG);

        int hllLength;
        Slice serializedHll;
        for (int i = 0; i < minhashSize; i++) {
            hllLength = valuesInput.readInt();
            serializedHll = Slices.allocate(hllLength);
            valuesInput.readBytes(serializedHll, hllLength);
            minhash.put(input.readLong(), HyperLogLog.newInstance(serializedHll));
        }

        return new KHyperLogLog(K, hllBuckets, minhash);
    }

    public Slice serialize()
    {
        try (SliceOutput output = new DynamicSliceOutput(estimatedSerializedSize())) {
            output.appendByte(UNCOMPRESSED_FORMAT);
            output.appendInt(K);
            output.appendInt(hllBuckets);
            output.appendInt(minhash.size());
            for (long key : minhash.keySet()) {
                output.appendLong(key);
            }
            Slice serializedHll;
            for (HyperLogLog hll : minhash.values()) {
                serializedHll = hll.serialize();
                output.appendInt(serializedHll.length());
                output.appendBytes(serializedHll);
            }

            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int estimatedInMemorySize()
    {
        return SIZE_OF_KHYPERLOGLOG +
                SIZE_OF_RBTREEMAP +
                minhash.size() * SIZE_OF_LONG +
                minhash.values().stream().mapToInt(HyperLogLog::estimatedInMemorySize).sum();
    }

    public int estimatedSerializedSize()
    {
        return SIZE_OF_BYTE +
                3 * SIZE_OF_INT +
                minhash.size() * (SIZE_OF_LONG + SIZE_OF_INT) +
                minhash.values().stream().mapToInt(HyperLogLog::estimatedSerializedSize).sum();
    }

    public boolean isExact()
    {
        return minhash.size() < K;
    }

    public long cardinality()
    {
        if (isExact()) {
            return minhash.size();
        }

        // Intuition is: get the stored hashes' density, and extrapolate to the whole Hash output range.
        // Since Hash output range (2^64) cannot be stored in long type, I use half of the range
        // via Long.MAX_VALUE and also divide the hash values' density by 2. The "-1" is bias correction
        // detailed in "On Synopses for Distinct-Value Estimation Under Multiset Operations" by Beyer et. al.
        long k_hashes_range = minhash.lastLongKey() - Long.MIN_VALUE;
        double half_density = Long.divideUnsigned(k_hashes_range,  minhash.size()) / 2D;
        return (long) (HASH_OUTPUT_HALF_RANGE / half_density);
    }

    public static long exactIntersectionCardinality(KHyperLogLog a, KHyperLogLog b)
    {
        // TODO: Review
        checkState(a.isExact(), "exact intersection cannot operate on approximate sets");
        checkArgument(b.isExact(), "exact intersection cannot operate on approximate sets");

        return Sets.intersection(a.minhash.keySet(), b.minhash.keySet()).size();
    }

    public static double jaccardIndex(KHyperLogLog a, KHyperLogLog b)
    {
        // TODO: Review
        int sizeOfSmallerSet = Math.min(a.minhash.size(), b.minhash.size());
        LongSortedSet minUnion = new LongRBTreeSet(a.minhash.keySet());
        minUnion.addAll(b.minhash.keySet());

        int intersection = 0;
        int i = 0;
        for (long key : minUnion) {
            if (a.minhash.containsKey(key) && b.minhash.containsKey(key)) {
                intersection++;
            }
            i++;
            if (i >= sizeOfSmallerSet) {
                break;
            }
        }
        return intersection / (double) sizeOfSmallerSet;
    }

    /*

    ADD SUPPORT FOR ADDING EVERY TYPE

     */

    public void add(long value, long uii)
    {
        update(Murmur3Hash128.hash64(value), uii);
    }

    public void add(Slice value, long uii)
    {
        update(Murmur3Hash128.hash64(value), uii);
    }

    private void update(long hash, long uii)
    {
        if (minhash.containsKey(hash)) {
            HyperLogLog hll = minhash.get(hash);
            hll.add(uii);
        } else if (isExact() || hash < minhash.lastLongKey()) {
            HyperLogLog hll = HyperLogLog.newInstance(this.hllBuckets);
            hll.add(uii);
            minhash.put(hash, hll);
            while (minhash.size() > K) {
                minhash.remove(minhash.lastLongKey());
            }
        }
    }

    public void mergeWith(KHyperLogLog other)
    {
        LongBidirectionalIterator iterator = other.minhash.keySet().iterator();
        while (iterator.hasNext()) {
            long key = iterator.nextLong();
            if (minhash.containsKey(key)) {
                minhash.get(key).mergeWith(other.minhash.get(key));
            } else {
                minhash.put(key, other.minhash.get(key));
            }
        }
        while (minhash.size() > K) {
            minhash.remove(minhash.lastLongKey());
        }
    }

    public Map<Long, HyperLogLog> getHashCounts()
    {
        return ImmutableMap.copyOf(minhash);
    }
}
