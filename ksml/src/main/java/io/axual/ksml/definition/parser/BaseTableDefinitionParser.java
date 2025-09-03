package io.axual.ksml.definition.parser;

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


import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyBaseResourceAwareParser;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.store.StoreType;
import io.axual.ksml.type.UserType;

import java.util.List;

public abstract class BaseTableDefinitionParser<T> extends TopologyBaseResourceAwareParser<T> {
    protected final boolean isJoinTarget;
    protected final String tableType;

    public BaseTableDefinitionParser(TopologyBaseResources resources, boolean isJoinTarget, String tableType) {
        super(resources);
        this.isJoinTarget = isJoinTarget;
        this.tableType = tableType != null && !tableType.isEmpty() ? tableType : "";
    }

    protected StructsParser<String> topicField() {
        return stringField(KSMLDSL.Streams.TOPIC, "The name of the Kafka topic for this " + tableType);

    }

    protected StructsParser<UserType> keyField() {
        return optional(userTypeField(KSMLDSL.Streams.KEY_TYPE, "The key type of the " + tableType), UserType.UNKNOWN);
    }

    protected StructsParser<UserType> valueField() {
        return optional(userTypeField(KSMLDSL.Streams.VALUE_TYPE, "The value type of the " + tableType), UserType.UNKNOWN);
    }

    protected StructsParser<FunctionDefinition> timestampExtractorField() {
        return optional(functionField(KSMLDSL.Streams.TIMESTAMP_EXTRACTOR, "A function that extracts the event time from a consumed record", new TimestampExtractorDefinitionParser(false)));
    }

    protected StructsParser<String> offsetResetPolicyField() {
        return optional(stringField(KSMLDSL.Streams.OFFSET_RESET_POLICY, "The policy that determines what to do when there is no initial consumer offset in Kafka, or if the message at the committed consumer offset does not exist (e.g. because that data has been deleted)"));
    }

    protected StructsParser<FunctionDefinition> partitionerField() {
        return optional(functionField(KSMLDSL.Streams.PARTITIONER, "A function that determines to which topic partition a given message needs to be written", new StreamPartitionerDefinitionParser(false)));
    }

    protected StructsParser<KeyValueStateStoreDefinition> storeField() {
        final var storeParser = new StateStoreDefinitionParser(StoreType.KEYVALUE_STORE, false);
        final var resourceParser = new TopologyResourceParser<>("state store", KSMLDSL.Streams.STORE, "KeyValue state store definition", null, storeParser);
        final var schemas = optional(resourceParser).schemas();
        return new StructsParser<>() {
            @Override
            public KeyValueStateStoreDefinition parse(ParseNode node) {
                storeParser.defaultShortName(node.name());
                storeParser.defaultLongName(node.longName());
                final var resource = resourceParser.parse(node);
                if (resource != null && resource.definition() instanceof KeyValueStateStoreDefinition def) return def;
                return null;
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }
}
