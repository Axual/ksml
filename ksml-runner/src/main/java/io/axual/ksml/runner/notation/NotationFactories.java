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
import io.axual.ksml.data.notation.avro.AvroSchemaLoader;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.csv.CsvSchemaLoader;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.soap.SOAPNotation;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaLoader;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class NotationFactories {
    public interface NotationFactory {
        Notation create(Map<String, String> notationConfig);
    }

    // Notation constants
    public static final String AVRO = "avro";
    public static final String APICURIO_AVRO = "apicurioAvro";
    public static final String CONFLUENT_AVRO = "confluentAvro";
    public static final String BINARY = "binary";
    public static final String CSV = "csv";
    public static final String JSON = "json";
    public static final String SOAP = "soap";
    public static final String XML = "xml";

    // Notation variables
    private final NotationFactory apicurioAvro;
    private final NotationFactory confluentAvro;
    private final NotationFactory binary;
    private final NotationFactory csv;
    private final NotationFactory json;
    private final NotationFactory soap;
    private final NotationFactory xml;

    private final Map<String, NotationFactory> notations = new HashMap<>();

    public NotationFactories(Map<String, String> kafkaConfig, String schemaDirectory) {
        // AVRO
        final var avroLoader = new AvroSchemaLoader(schemaDirectory);
        final var nativeMapper = new DataObjectFlattener(true);
        apicurioAvro = (notationConfig) -> new AvroNotation(AvroNotation.SerdeType.APICURIO, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig), avroLoader);
        notations.put(APICURIO_AVRO, apicurioAvro);
        confluentAvro = (notationConfig) -> new AvroNotation(AvroNotation.SerdeType.CONFLUENT, nativeMapper, MapUtil.merge(kafkaConfig, notationConfig), avroLoader);
        notations.put(CONFLUENT_AVRO, confluentAvro);

        // CSV
        csv = (config) -> new CsvNotation(nativeMapper, new CsvSchemaLoader(schemaDirectory));
        notations.put(CSV, csv);

        // JSON
        json = (config) -> new JsonNotation(nativeMapper, new JsonSchemaLoader(schemaDirectory));
        notations.put(JSON, json);

        // SOAP
        soap = (config) -> new SOAPNotation(nativeMapper);
        notations.put(SOAP, soap);

        // XML
        xml = (config) -> new XmlNotation(nativeMapper, new XmlSchemaLoader(schemaDirectory));
        notations.put(XML, xml);

        // Binary
        binary = (config) -> new BinaryNotation(nativeMapper, json.create(null)::serde);
        notations.put(BINARY, binary);
    }
}
