package io.axual.ksml.operation;

import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.HashMap;

public class BaseJoinOperation extends StoreOperation {
    protected BaseJoinOperation(StoreOperationConfig config) {
        super(config);
    }

    protected Joined<Object, Object, Object> joinedOf(String name, StreamDataType k, StreamDataType v, StreamDataType vo, Duration gracePeriod) {
        if (gracePeriod != null) {
            var joined = Joined.with(k.serde(), v.serde(), vo.serde());
            if (name != null) joined = joined.withName(name);
            return joined.withGracePeriod(gracePeriod);
        }
        return Joined.with(null, null, null);
    }

    protected StreamJoined<Object, Object, Object> streamJoinedOf(WindowStateStoreDefinition store, StreamDataType k, StreamDataType v, StreamDataType vo) {
        if (store != null) {
            var joined = StreamJoined.with(k.serde(), v.serde(), vo.serde());
            if (name != null) joined = joined.withName(name);
            if (store.name() != null) joined = joined.withStoreName(store.name());
            return store.logging() ? joined.withLoggingEnabled(new HashMap<>()) : joined.withLoggingDisabled();
        }

        return StreamJoined.with(null, null, null).withLoggingDisabled();
    }

    protected JoinWindows joinWindowsOf(Duration timeDifference, Duration gracePeriod) {
        if (gracePeriod != null) {
            return JoinWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod);
        } else {
            return JoinWindows.ofTimeDifferenceWithNoGrace(timeDifference);
        }
    }
}
