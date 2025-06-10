package io.axual.ksml.util;

import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class JoinUtil {
    private JoinUtil() {
    }

    public static Joined<Object, Object, Object> joinedOf(String name, StreamDataType k, StreamDataType v, StreamDataType vo, Duration gracePeriod) {
        var result = Joined.with(k.serde(), v.serde(), vo.serde());
        if (name != null) result = result.withName(name);
        if (gracePeriod != null) result = result.withGracePeriod(gracePeriod);
        return result;
    }

    public static StreamJoined<Object, Object, Object> streamJoinedOf(String name, WindowStateStoreDefinition thisStore, WindowStateStoreDefinition otherStore, StreamDataType k, StreamDataType v, StreamDataType vo) {
        var result = StreamJoined.with(k.serde(), v.serde(), vo.serde()).withLoggingDisabled();
        if (name != null) result = result.withName(name);
        if (thisStore != null) {
            if (thisStore.name() != null) result = result.withStoreName(thisStore.name());
            if (thisStore.logging()) result = result.withLoggingEnabled(new HashMap<>());
            result = result.withThisStoreSupplier(StoreUtil.getStoreSupplier(thisStore));
        }
        if (otherStore != null) {
            result = result.withOtherStoreSupplier(StoreUtil.getStoreSupplier(otherStore));
        }
        return result;
    }

    private record WrapPartitioner(
            StreamPartitioner<Object, Object> partitioner) implements StreamPartitioner<Object, Void> {
        @Override
        public Optional<Set<Integer>> partitions(String topic, Object key, Void value, int numPartitions) {
            return partitioner.partitions(topic, key, value, numPartitions);
        }
    }

    public static TableJoined<Object, Object> tableJoinedOf(String name, StreamPartitioner<Object, Object> partitioner, StreamPartitioner<Object, Object> otherPartitioner) {
        final var part = partitioner != null ? new WrapPartitioner(partitioner) : null;
        final var otherPart = otherPartitioner != null ? new WrapPartitioner(otherPartitioner) : null;
        return TableJoined.with(part, otherPart).withName(name);
    }

    public static JoinWindows joinWindowsOf(Duration timeDifference, Duration gracePeriod) {
        if (gracePeriod != null) {
            return JoinWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod);
        }
        return JoinWindows.ofTimeDifferenceWithNoGrace(timeDifference);
    }

    public static ValueJoiner<Object, Object, Object> valueJoiner(UserFunction function, MetricTags tags) {
        return new UserValueJoiner(function, tags);
    }

    public static ValueJoinerWithKey<Object, Object, Object, Object> valueJoinerWithKey(UserFunction function, MetricTags tags) {
        return new UserValueJoiner(function, tags);
    }
}
