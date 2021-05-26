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


import org.apache.kafka.streams.StreamsBuilder;

import io.axual.ksml.generator.SerdeGenerator;
import io.axual.ksml.parser.TypeParser;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.type.DataType;

public abstract class BaseStreamDefinition {
    public final String topic;
    public final DataType keyType;
    public final DataType valueType;

    public BaseStreamDefinition(String topic, String keyType, String valueType) {
        this(topic, TypeParser.parse(keyType), TypeParser.parse(valueType));
    }

    public BaseStreamDefinition(String topic, DataType keyType, DataType valueType) {
        this.topic = topic;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    /**
     * Add definitions for this stream definition to the provided builder, and returns a wrapped stream.
     *
     * @param builder        the StreamsBuilder to add the stream to.
     * @param serdeGenerator serde generator for the key and value types.
     */
    public abstract StreamWrapper addToBuilder(StreamsBuilder builder, String name, SerdeGenerator serdeGenerator);
}
