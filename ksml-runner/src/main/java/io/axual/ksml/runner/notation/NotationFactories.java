package io.axual.ksml.runner.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.client.util.MapUtil;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.NotationProvider;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.AvroSerdeSupplier;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaSerdeSupplier;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.protobuf.ProtobufSerdeSupplier;
import io.axual.ksml.data.notation.soap.SoapNotation;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.type.UserType;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@Getter
public class NotationFactories {
    public interface NotationFactory {
        Notation create(Map<String, String> notationConfig);
    }

    // Notation constants
    private final Map<String, NotationFactory> notations = new HashMap<>();

    public NotationFactories(Map<String, String> kafkaConfig) {
        final var nativeMapper = new DataObjectFlattener();

        // Load all notations
        for (final var provider : ServiceLoader.load(NotationProvider.class)) {
            notations.put(provider.name(), configs -> {
                final var context = new NotationContext(nativeMapper, MapUtil.merge(kafkaConfig, configs));
                return provider.createNotation(context);
            });
        }

        // Get the JSON notation
        final NotationFactory json = notations.get(JsonNotation.NOTATION_NAME);
        // Create the binary notation, with JSON for complex types, and set it as default
        final NotationFactory binary = config -> new BinaryNotation(nativeMapper, json.create(null)::serde);
        notations.put(BinaryNotation.NOTATION_NAME, binary);
        notations.put(UserType.DEFAULT_NOTATION, binary);
    }
}
