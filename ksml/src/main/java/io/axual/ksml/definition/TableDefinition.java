package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import io.axual.ksml.data.type.UserType;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.store.StateStoreRegistry;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

public class TableDefinition extends BaseStreamDefinition {
    public final KeyValueStateStoreDefinition store;

    public TableDefinition(String topic, String keyType, String valueType, KeyValueStateStoreDefinition store) {
        this(topic, UserTypeParser.parse(keyType), UserTypeParser.parse(valueType), store);
    }

    public TableDefinition(String topic, UserType keyType, UserType valueType, KeyValueStateStoreDefinition store) {
        super(topic, keyType, valueType);
        this.store = store;
    }

    @Override
    public StreamWrapper addToBuilder(StreamsBuilder builder, String name, NotationLibrary notationLibrary, StateStoreRegistry storeRegistry) {
        final var streamKey = new StreamDataType(notationLibrary, keyType, true);
        final var streamValue = new StreamDataType(notationLibrary, valueType, false);

        if (store != null) {
            // Register the state store and mark as already created (by Kafka Streams framework, not by user)
            if (storeRegistry != null) storeRegistry.registerStateStore(store);
            final var mat = StoreUtil.materialize(store, notationLibrary);
            return new KTableWrapper(builder.table(topic, mat), streamKey, streamValue);
        }

        final var consumed = Consumed.as(name).withKeySerde(streamKey.getSerde()).withValueSerde(streamValue.getSerde());
        return new KTableWrapper(builder.table(topic, consumed), streamKey, streamValue);
    }
}
