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


import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.python.util.PythonInterpreter;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.generator.TopologyGeneratorImpl;
import io.axual.ksml.parser.SchemaLoader;

/**
 * Generates a Kafka Streams topology based on a KSML config file.
 * @see KSMLConfig
 */
public class KSMLTopologyGenerator implements Configurable, TopologyGenerator {
    private KSMLConfig config = new KSMLConfig(new HashMap<>());

    @Override
    public void configure(Map<String, ?> configs) {
        // Parse configuration
        this.config = new KSMLConfig(configs);
    }

    @Override
    public Topology create(StreamsBuilder streamsBuilder) {
        SchemaLoader.setSchemaDirectory(config.workingDirectory);
        var generator = new TopologyGeneratorImpl(config, config.interpreterIsolation ? null : new PythonInterpreter());
        return generator.create(streamsBuilder);
    }
}
