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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.Slice;

@AggregationFunction("make_khyperloglog")
public final class BuildKHyperLogLogAggregation
{
    private static final KHyperLogLogStateSerializer SERIALIZER = new KHyperLogLogStateSerializer();

    private BuildKHyperLogLogAggregation() {}

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        if (state.getKHLL() == null) {
            state.setKHLL(new KHyperLogLog());
        }
        state.getKHLL().add(value, uii);
    }

    @InputFunction
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long uii)
    {
        input(state, Double.doubleToLongBits(value), uii);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType("varchar(x)") Slice uii)
    {
        input(state, value, Murmur3Hash128.hash64(uii));
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(@AggregationState KHyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType("varchar(y)") Slice uii)
    {
        input(state, value, Murmur3Hash128.hash64(uii));
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState KHyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("varchar(x)") Slice uii)
    {
        input(state, Double.doubleToLongBits(value), Murmur3Hash128.hash64(uii));
    }

    @CombineFunction
    public static void combine(@AggregationState KHyperLogLogState state, @AggregationState KHyperLogLogState otherState)
    {
        if (state.getKHLL() == null) {
            KHyperLogLog copy = new KHyperLogLog();
            copy.mergeWith(otherState.getKHLL());
            state.setKHLL(copy);
        }
        else {
            state.getKHLL().mergeWith(otherState.getKHLL());
        }
    }

    @OutputFunction(KHyperLogLogType.NAME)
    public static void output(@AggregationState KHyperLogLogState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
