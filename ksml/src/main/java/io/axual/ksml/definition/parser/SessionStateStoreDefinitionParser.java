package io.axual.ksml.definition.parser;

import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

public class SessionStateStoreDefinitionParser extends DefinitionParser<SessionStateStoreDefinition> implements NamedObjectParser {
    private final boolean requireType;
    private String defaultName;

    public SessionStateStoreDefinitionParser(boolean requireType) {
        this.requireType = requireType;
    }

    @Override
    protected StructParser<SessionStateStoreDefinition> parser() {
        return structParser(
                SessionStateStoreDefinition.class,
                "Definition of a session state store",
                stringField(KSMLDSL.Stores.TYPE, requireType, "The type of the state store, fixed value \"" + StoreType.SESSION_STORE + "\""),
                stringField(KSMLDSL.Stores.NAME, false, null, "The name of the session store. If this field is not defined, then the name is derived from the context."),
                booleanField(KSMLDSL.Stores.PERSISTENT, false, "\"true\" if this session store needs to be stored on disk, \"false\" otherwise"),
                booleanField(KSMLDSL.Stores.TIMESTAMPED, false, "\"true\" if elements in the store are timestamped, \"false\" otherwise"),
                durationField(KSMLDSL.Stores.RETENTION, false, "The duration for which elements in the session store are retained"),
                userTypeField(KSMLDSL.Stores.KEY_TYPE, false, "The key type of the session store"),
                userTypeField(KSMLDSL.Stores.VALUE_TYPE, false, "The value type of the session store"),
                booleanField(KSMLDSL.Stores.CACHING, false, "\"true\" if changed to the session store need to be buffered and periodically released, \"false\" to emit all changes directly"),
                booleanField(KSMLDSL.Stores.LOGGING, false, "\"true\" if a changelog topic should be set up on Kafka for this session store, \"false\" otherwise"),
                (type, name, persistent, timestamped, retention, keyType, valueType, caching, logging) -> {
                    // Validate the type field if one was provided
                    if (type != null && !StoreType.SESSION_STORE.externalName().equals(type)) {
                        throw FatalError.topologyError("Expected store type \"" + StoreType.SESSION_STORE.externalName() + "\"");
                    }
                    name = name != null ? name : defaultName;
                    if (name == null || name.isEmpty()) {
                        throw FatalError.topologyError("State store name not defined");
                    }
                    return new SessionStateStoreDefinition(name, persistent, timestamped, retention, keyType, valueType, caching, logging);
                });
    }

    @Override
    public void defaultName(String name) {
        defaultName = name;
    }
}
