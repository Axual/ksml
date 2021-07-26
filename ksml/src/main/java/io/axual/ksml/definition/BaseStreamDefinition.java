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

import io.axual.ksml.notation.BinaryNotation;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.TypeParser;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.DataTypeAndNotation;

public abstract class BaseStreamDefinition {
    public final String topic;
    public final DataType keyType;
    public final String keyNotation;
    public final DataType valueType;
    public final String valueNotation;

    public BaseStreamDefinition(String topic, String keyType, String valueType) {
        this(topic, TypeParser.parse(keyType, BinaryNotation.NAME), TypeParser.parse(valueType, BinaryNotation.NAME));
    }

    public BaseStreamDefinition(String topic, DataTypeAndNotation keyType, DataTypeAndNotation valueType) {
        this(topic, keyType.type, keyType.notation, valueType.type, valueType.notation);
    }

    public BaseStreamDefinition(String topic, DataType keyType, String keyNotation, DataType valueType, String valueNotation) {
        this.topic = topic;
        this.keyType = keyType;
        this.keyNotation = keyNotation;
        this.valueType = valueType;
        this.valueNotation = valueNotation;
    }

    /**
     * Add definitions for this stream definition to the provided builder, and returns a wrapped stream.
     *
     * @param builder         the StreamsBuilder to add the stream to.
     * @param notationLibrary notation library  generator for the key and value types.
     */
    public abstract StreamWrapper addToBuilder(StreamsBuilder builder, String name, NotationLibrary notationLibrary);
}
