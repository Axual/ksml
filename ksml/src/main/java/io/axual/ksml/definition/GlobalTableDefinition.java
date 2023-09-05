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


import io.axual.ksml.store.StateStoreRegistry;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import io.axual.ksml.data.type.UserType;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.StreamWrapper;

public class GlobalTableDefinition extends BaseStreamDefinition {
    public GlobalTableDefinition(String topic, String keyType, String valueType) {
        this(topic, UserTypeParser.parse(keyType), UserTypeParser.parse(valueType));
    }

    public GlobalTableDefinition(String topic, UserType keyType, UserType valueType) {
        super(topic, keyType, valueType);
    }

    @Override
    public StreamWrapper addToBuilder(StreamsBuilder builder, String name, NotationLibrary notationLibrary, StateStoreRegistry storeRegistry) {
        var streamKey = new StreamDataType(notationLibrary, keyType, true);
        var streamValue = new StreamDataType(notationLibrary, valueType, false);
        return new GlobalKTableWrapper(
                builder.globalTable(topic, Consumed.with(streamKey.getSerde(), streamValue.getSerde()).withName(name)),
                streamKey,
                streamValue);
    }
}
