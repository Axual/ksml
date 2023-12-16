package io.axual.ksml.definition.parser;

import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
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
                fixedStringField(KSMLDSL.Stores.TYPE, requireType, StoreType.KEYVALUE_STORE.externalName(), "The type of the state store"),
                stringField(KSMLDSL.Stores.NAME, false, null, "The name of the keyValue store. If this field is not defined, then the name is derived from the context."),
                booleanField(KSMLDSL.Stores.PERSISTENT, false, "\"true\" if this keyValue store needs to be stored on disk, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.TIMESTAMPED, false, "\"true\" if elements in the store are timestamped, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.VERSIONED, false, "\"true\" if elements in the store are versioned, \"false\" otherwise"),
                durationField(KSMLDSL.Stores.HISTORY_RETENTION, false, "(Versioned only) The duration for which old record versions are available for query (cannot be negative)"),
                durationField(KSMLDSL.Stores.SEGMENT_INTERVAL, false, "Size of segments for storing old record versions (must be positive). Old record versions for the same key in a single segment are stored (updated and accessed) together. The only impact of this parameter is performance. If segments are large and a workload results in many record versions for the same key being collected in a single segment, performance may degrade as a result. On the other hand, historical reads (which access older segments) and out-of-order writes may slow down if there are too many segments."),
                userTypeField(KSMLDSL.Stores.KEY_TYPE, false, "The key type of the keyValue store"),
                userTypeField(KSMLDSL.Stores.VALUE_TYPE, false, "The value type of the keyValue store"),
                booleanField(KSMLDSL.Stores.CACHING, false, "\"true\" if changed to the keyValue store need to be buffered and periodically released, \"false\" to emit all changes directly"),
                booleanField(KSMLDSL.Stores.LOGGING, false, "\"true\" if a changelog topic should be set up on Kafka for this keyValue store, \"false\" otherwise"),
                (type, name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging) -> {
                    // Validate the type field if one was provided
                    if (type != null && !StoreType.KEYVALUE_STORE.externalName().equals(type)) {
                        return parseError("Expected store type \"" + StoreType.KEYVALUE_STORE.externalName() + "\"");
                    }
                    name = validateName("KeyValue state store", name, defaultName);
                    return new KeyValueStateStoreDefinition(name, persistent, timestamped, versioned, history, segment, keyType, valueType, caching, logging);
                });
    }

    @Override
    public void defaultName(String name) {
        defaultName = name;
    }
}
