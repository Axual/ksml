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
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.notation.protobuf.ProtobufNotation;
import io.axual.ksml.data.notation.soap.SoapNotation;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.type.UserType;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class NotationFactories {
    public interface NotationFactory {
        Notation create(Map<String, String> notationConfig);
    }

    // Notation constants
    private final Map<String, NotationFactory> notations = new HashMap<>();

    public NotationFactories(Map<String, String> kafkaConfig) {
        final var nativeMapper = new DataObjectFlattener();

        // AVRO
        for (final var provider : AvroNotation.getSerdeProviders()) {
            notations.put(provider.name(), notationConfig -> new AvroNotation(provider, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig)));
        }

        // CSV
        notations.put(CsvNotation.NOTATION_NAME, config -> new CsvNotation(nativeMapper));

        // JSON
        final NotationFactory json = config -> new JsonNotation(nativeMapper);
        notations.put(JsonNotation.NOTATION_NAME, json);

        // JSON Schema
        for (final var provider : JsonSchemaNotation.getSerdeProviders()) {
            notations.put(provider.name(), notationConfig -> new JsonSchemaNotation(provider, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig)));
        }

        // Protobuf
        for (final var provider : ProtobufNotation.getSerdeProviders()) {
            final var dataObjectMapper = new ProtobufDataObjectMapper(provider.fileElementMapper());
            final var schemaParser = provider.schemaParser();
            notations.put(provider.name(), notationConfig -> new ProtobufNotation(provider, schemaParser, dataObjectMapper, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig)));
        }

        // SOAP
        notations.put(SoapNotation.NOTATION_NAME, config -> new SoapNotation(nativeMapper));

        // XML
        notations.put(XmlNotation.NOTATION_NAME, config -> new XmlNotation(nativeMapper));

        // Binary for simple types, complex types are serialized in JSON format

        final NotationFactory binary = config -> new BinaryNotation(nativeMapper, json.create(null)::serde);
        notations.put(BinaryNotation.NOTATION_NAME, binary);
        notations.put(UserType.DEFAULT_NOTATION, binary);
    }
}
