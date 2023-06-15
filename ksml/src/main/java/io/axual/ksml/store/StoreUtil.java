package io.axual.ksml.store;

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;

public class StoreUtil {
    private StoreUtil() {
    }

    public static <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> materialize(KeyValueStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, KeyValueStore<Bytes, byte[]>> result = Materialized.as(store.name());
        return materialize(result, store, notationLibrary);
    }

    public static <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> materialize(SessionStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, SessionStore<Bytes, byte[]>> mat = Materialized.as(store.name());
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, notationLibrary);
    }

    public static <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> materialize(WindowStateStoreDefinition store, NotationLibrary notationLibrary) {
        Materialized<Object, V, WindowStore<Bytes, byte[]>> mat = Materialized.as(store.name());
        if (store.retention() != null) mat = mat.withRetention(store.retention());
        return materialize(mat, store, notationLibrary);
    }

    private static <V, S extends StateStore> Materialized<Object, V, S> materialize(Materialized<Object, V, S> mat, StateStoreDefinition store, NotationLibrary notationLibrary) {
        var keyType = new StreamDataType(notationLibrary, store.keyType(), true);
        var valueType = new StreamDataType(notationLibrary, store.valueType(), false);
        mat = mat.withKeySerde(keyType.getSerde()).withValueSerde((Serde<V>) valueType.getSerde());
        mat = store.caching() ? mat.withCachingEnabled() : mat.withCachingDisabled();
        mat = store.logging() ? mat.withLoggingEnabled(new HashMap<>()) : mat.withLoggingDisabled();
        return mat;
    }
}
