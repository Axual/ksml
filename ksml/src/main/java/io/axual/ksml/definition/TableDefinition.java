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


import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.store.StoreRegistry;
import io.axual.ksml.store.StoreType;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class TableDefinition extends BaseStreamDefinition {
    private final boolean queryable;
    private final StoreRegistry storeRegistry;

    public TableDefinition(String topic, String keyType, String valueType, boolean queryable, StoreRegistry storeRegistry) {
        this(topic, UserTypeParser.parse(keyType), UserTypeParser.parse(valueType), queryable, storeRegistry);
    }

    public TableDefinition(String topic, UserType keyType, UserType valueType, boolean queryable, StoreRegistry storeRegistry) {
        super(topic, keyType, valueType);
        this.queryable = queryable;
        this.storeRegistry = storeRegistry;
    }

    @Override
    public StreamWrapper addToBuilder(StreamsBuilder builder, String name, NotationLibrary notationLibrary) {
        var streamKey = new StreamDataType(notationLibrary, keyType, true);
        var streamValue = new StreamDataType(notationLibrary, valueType, false);

        if (queryable) {
            var store = new StoreDefinition(name, null, null);
            storeRegistry.registerStore(StoreType.KEYVALUE_STORE, store, streamKey, streamValue);
            var mat = StoreUtil.createKeyValueStore(store, streamKey, streamValue);
            return new KTableWrapper(builder.table(topic, mat), streamKey, streamValue);
        }

        var keySerde = streamKey.getSerde();
        var valueSerde = streamValue.getSerde();

        var materialized = Materialized
                .<Object, Object, KeyValueStore<Bytes, byte[]>>as(name)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde)
                .withStoreType(Materialized.StoreType.IN_MEMORY);
        var consumed = Consumed.with(keySerde, valueSerde).withName(name);
        return new KTableWrapper(builder.table(topic, consumed, materialized), streamKey, streamValue);
    }
}
