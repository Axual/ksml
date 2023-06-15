package io.axual.ksml.definition;

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.store.StoreType;

public class KeyValueStateStoreDefinition extends StateStoreDefinition {
    public KeyValueStateStoreDefinition(String name, Boolean persistent, Boolean timestamped, UserType keyType, UserType valueType, Boolean caching, Boolean logging) {
        super(StoreType.KEYVALUE_STORE, name, persistent, timestamped, keyType, valueType, caching, logging);
    }
}
