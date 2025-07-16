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
        Notation create(String name, Map<String, String> notationConfig);
    }

    // Notation constants
    public static final String AVRO = "avro";
    public static final String APICURIO_AVRO = "apicurio_avro";
    public static final String APICURIO_JSONSCHEMA = "apicurio_jsonschema";
    public static final String APICURIO_PROTOBUF = "apicurio_protobuf";
    public static final String CONFLUENT_AVRO = "confluent_avro";
    public static final String CONFLUENT_JSONSCHEMA = "confluent_jsonschema";
    public static final String CONFLUENT_PROTOBUF = "confluent_protobuf";
    public static final String BINARY_NOTATION = "binary";
    public static final String CSV_NOTATION = "csv";
    public static final String DEFAULT_NOTATION = UserType.DEFAULT_NOTATION;
    public static final String JSON_NOTATION = "json";
    public static final String SOAP_NOTATION = "soap";
    public static final String XML_NOTATION = "xml";

    // Notation variables
    private final NotationFactory apicurioAvro;
    private final NotationFactory apicurioJsonSchema;
    private final NotationFactory apicurioProtobuf;
    private final NotationFactory confluentAvro;
    private final NotationFactory confluentJsonSchema;
    private final NotationFactory confluentProtobuf;
    private final NotationFactory binary;
    private final NotationFactory csv;
    private final NotationFactory json;
    private final NotationFactory soap;
    private final NotationFactory xml;

    private final Map<String, NotationFactory> notations = new HashMap<>();

    public NotationFactories(Map<String, String> kafkaConfig) {
        final var nativeMapper = new DataObjectFlattener();

        // AVRO
        apicurioAvro = (name, notationConfig) -> new AvroNotation(name, AvroNotation.SerdeType.APICURIO, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig));
        notations.put(APICURIO_AVRO, apicurioAvro);
        confluentAvro = (name, notationConfig) -> new AvroNotation(name, AvroNotation.SerdeType.CONFLUENT, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig));
        notations.put(CONFLUENT_AVRO, confluentAvro);

        // CSV
        csv = (name, config) -> new CsvNotation(name, nativeMapper);
        notations.put(CSV_NOTATION, csv);

        // JSON
        json = (name, config) -> new JsonNotation(name, nativeMapper);
        notations.put(JSON_NOTATION, json);

        // JSON Schema
        apicurioJsonSchema = (name, config) -> new JsonSchemaNotation(name, JsonSchemaNotation.SerdeType.APICURIO, nativeMapper, MapUtil.merge(kafkaConfig, config), null);
        notations.put(APICURIO_JSONSCHEMA, apicurioJsonSchema);
        confluentJsonSchema = (name, config) -> new JsonSchemaNotation(name, JsonSchemaNotation.SerdeType.CONFLUENT, nativeMapper, MapUtil.merge(kafkaConfig, config), null);
        notations.put(CONFLUENT_JSONSCHEMA, confluentJsonSchema);

        // Protobuf
        apicurioProtobuf = (name, config) -> new ProtobufNotation(name, ProtobufNotation.SerdeType.APICURIO, nativeMapper, MapUtil.merge(kafkaConfig, config));
        notations.put(APICURIO_PROTOBUF, apicurioProtobuf);
        confluentProtobuf = (name, config) -> new ProtobufNotation(name, ProtobufNotation.SerdeType.CONFLUENT, nativeMapper, MapUtil.merge(kafkaConfig, config));
        notations.put(CONFLUENT_PROTOBUF, confluentProtobuf);

        // SOAP
        soap = (name, config) -> new SoapNotation(name, nativeMapper);
        notations.put(SOAP_NOTATION, soap);

        // XML
        xml = (name, config) -> new XmlNotation(name, nativeMapper);
        notations.put(XML_NOTATION, xml);

        // Binary for simple types, complex types are serialized in JSON format
        binary = (name, config) -> new BinaryNotation(name, nativeMapper, json.create(name, null)::serde);
        notations.put(BINARY_NOTATION, binary);
        notations.put(DEFAULT_NOTATION, binary);
    }
}
