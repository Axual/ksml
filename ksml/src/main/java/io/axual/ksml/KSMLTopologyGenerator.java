package io.axual.ksml;

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


import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.generator.TopologyGeneratorImpl;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.avro.AvroSchemaLoader;
import io.axual.ksml.notation.csv.CsvNotation;
import io.axual.ksml.notation.csv.CsvSchemaLoader;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.json.JsonSchemaLoader;
import io.axual.ksml.notation.xml.XmlNotation;
import io.axual.ksml.notation.xml.XmlSchemaLoader;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Map;
import java.util.Properties;

/**
 * Generates a Kafka Streams topology based on a KSML config file.
 *
 * @see KSMLConfig
 */
public class KSMLTopologyGenerator implements TopologyGenerator {
    private final String applicationId;
    private final KSMLConfig config;
    private final Properties kafkaConfig = new Properties();

    public KSMLTopologyGenerator(String applicationId, KSMLConfig ksmlConfig, Properties kafkaConfigs) {
        // Parse configuration
        this.applicationId = applicationId;
        this.config = ksmlConfig;
        this.kafkaConfig.putAll(kafkaConfigs);
    }

    @Override
    public Topology create(StreamsBuilder streamsBuilder) {
        // Register schema loaders
        SchemaLibrary.registerLoader(AvroNotation.NOTATION_NAME, new AvroSchemaLoader(config.configDirectory));
        SchemaLibrary.registerLoader(CsvNotation.NOTATION_NAME, new CsvSchemaLoader(config.configDirectory));
        SchemaLibrary.registerLoader(JsonNotation.NOTATION_NAME, new JsonSchemaLoader(config.configDirectory));
        SchemaLibrary.registerLoader(XmlNotation.NOTATION_NAME, new XmlSchemaLoader(config.configDirectory));

        // Create the topology generator
        var generator = new TopologyGeneratorImpl(config);

        // Parse and return the topology
        return generator.create(applicationId, streamsBuilder);
    }
}
