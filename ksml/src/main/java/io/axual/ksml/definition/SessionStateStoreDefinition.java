package io.axual.ksml.definition;

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.store.StoreType;

import java.time.Duration;

public class SessionStateStoreDefinition extends StateStoreDefinition {
    private final Duration retention;

    public SessionStateStoreDefinition(String name, Boolean persistent, Boolean timestamped, Duration retention, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        super(StoreType.SESSION_STORE, name, persistent, timestamped, keyType, valueType, caching, logging);
        this.retention = retention;
    }

    public Duration retention() {
        return retention;
    }
}
