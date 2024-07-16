package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.HashMap;

public abstract class BaseJoinOperation extends StoreOperation {
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

    protected ValueJoiner<Object, Object, Object> valueJoiner(UserFunction function, ContextTags tags) {
        return new UserValueJoiner(function, tags);
    }

    protected ValueJoinerWithKey<Object, Object, Object, Object> valueJoinerWithKey(UserFunction function, ContextTags tags) {
        return new UserValueJoiner(function, tags);
    }
}
