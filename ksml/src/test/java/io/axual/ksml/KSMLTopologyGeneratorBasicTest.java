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


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.python.util.PythonInterpreter;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.generator.TopologyGeneratorImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


/**
 * Basic "golden master" test for topology generation.
 * Generate a topolgy and compare with stored string representation. Reference output is in src/test/resources/reference.
 */
public class KSMLTopologyGeneratorBasicTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    public void parseAndCheckOuput(int nr) throws Exception {
        final URI uri = ClassLoader.getSystemResource("pipelines/" + nr + "-demo.yaml").toURI();
        final Path path = Paths.get(uri);
        String pipeDefinition = Files.readString(path);
        Map<String, Object> configs = new HashMap<>();
        configs.put(KSMLConfig.KSML_SOURCE_TYPE, "content");
        configs.put(KSMLConfig.KSML_SOURCE, pipeDefinition);
        TopologyGeneratorImpl topologyGenerator = new TopologyGeneratorImpl(new KSMLConfig(configs), new PythonInterpreter());
        final Topology topology = topologyGenerator.create(new StreamsBuilder());
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        URI referenceURI = ClassLoader.getSystemResource("reference/" + nr + "-reference.txt").toURI();
        String reference = Files.readString(Paths.get(referenceURI));

        // TODO this will not always work reliably, since the topology string can have direct object reference.
        // these will be different every time, need a workaround for this. For now visual check will have to do.
        assertThat(description.toString(), is(reference));
    }
}
