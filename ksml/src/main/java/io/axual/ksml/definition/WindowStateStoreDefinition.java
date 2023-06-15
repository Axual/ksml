package io.axual.ksml.definition;

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.store.StoreType;

import java.time.Duration;

public class WindowStateStoreDefinition extends StateStoreDefinition {
    private final Duration retention;
    private final Duration windowSize;
    private final Boolean retainDuplicates;

    public WindowStateStoreDefinition(String name, Boolean persistent, Boolean timestamped, Duration retention, Duration windowSize, Boolean retainDuplicates, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        super(StoreType.WINDOW_STORE, name, persistent, timestamped, keyType, valueType, caching, logging);
        this.retention = retention;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
    }

    public Duration retention() {
        return retention;
    }

    public Duration windowSize() {
        return windowSize;
    }

    public boolean retainDuplicates() {
        return retainDuplicates != null && retainDuplicates;
    }
}
