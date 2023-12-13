package io.axual.ksml.definition.parser;

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

public class KeyValueStateStoreDefinitionParser extends DefinitionParser<KeyValueStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireType;
    private String defaultName;

    public KeyValueStateStoreDefinitionParser(boolean requireType) {
        this.requireType = requireType;
    }

    @Override
    protected StructParser<KeyValueStateStoreDefinition> parser() {
        return structParser(
                KeyValueStateStoreDefinition.class,
                "Definition of a keyValue state store",
                stringField(KSMLDSL.Stores.TYPE, requireType, "The type of the state store, fixed value \"" + StoreType.KEYVALUE_STORE + "\""),
                stringField(KSMLDSL.Stores.NAME, false, null, "The name of the keyValue store. If this field is not defined, then the name is derived from the context."), booleanField(KSMLDSL.Stores.PERSISTENT, false, "\"true\" if this keyValue store needs to be stored on disk, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.TIMESTAMPED, false, "\"true\" if elements in the store are timestamped, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.VERSIONED, false, "\"true\" if elements in the store are versioned, \"false\" otherwise"),
                durationField(KSMLDSL.Stores.HISTORY_RETENTION, false, "The duration for which elements in the keyValue store are retained"),
                durationField(KSMLDSL.Stores.SEGMENT_INTERVAL, false, "Size of segments for storing old record versions (must be positive)"),
                userTypeField(KSMLDSL.Stores.KEY_TYPE, false, "The key type of the keyValue store"),
                userTypeField(KSMLDSL.Stores.VALUE_TYPE, false, "The value type of the keyValue store"),
                booleanField(KSMLDSL.Stores.CACHING, false, "\"true\" if changed to the keyValue store need to be buffered and periodically released, \"false\" to emit all changes directly"),
                booleanField(KSMLDSL.Stores.LOGGING, false, "\"true\" if a changelog topic should be set up on Kafka for this keyValue store, \"false\" otherwise"),
                (type, name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging) -> {
                    // Validate the type field if one was provided
                    if (type != null && !StoreType.KEYVALUE_STORE.externalName().equals(type)) {
                        throw FatalError.topologyError("Expected store type \"" + StoreType.KEYVALUE_STORE.externalName() + "\"");
                    }
                    name = name != null ? name : defaultName;
                    if (name == null || name.isEmpty()) {
                        throw FatalError.topologyError("State store name not defined");
                    }
                    return new KeyValueStateStoreDefinition(name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging);
                });
    }

    @Override
    public void defaultName(String name) {
        defaultName = name;
    }
}
