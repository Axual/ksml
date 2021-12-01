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
import org.apache.kafka.streams.TopologyDescription;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.generator.TopologyGeneratorImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * Basic "golden master" test for topology generation.
 * Generate a topolgy and compare with stored string representation. Reference output is in src/test/resources/reference.
 */
public class KSMLTopologyGeneratorBasicTest {

    @BeforeAll
    public static void checkGraalVM() {
        final var vendor = System.getProperty("java.vendor.url");
        if (!vendor.contains("graalvm")) {
            fail("This test needs GraalVM to work");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    public void parseAndCheckOuput(int nr) throws Exception {
        final URI uri = ClassLoader.getSystemResource("pipelines/" + nr + "-demo.yaml").toURI();
        final Path path = Paths.get(uri);
        String pipeDefinition = Files.readString(path);
        Map<String, Object> configs = new HashMap<>();
        configs.put(KSMLConfig.KSML_SOURCE_TYPE, "content");
        configs.put(KSMLConfig.KSML_SOURCE, pipeDefinition);
        TopologyGeneratorImpl topologyGenerator = new TopologyGeneratorImpl(new KSMLConfig(configs));
        final var result = topologyGenerator.create(new StreamsBuilder());
        final TopologyDescription description = result.getTopology().describe();
        System.out.println(description);

        URI referenceURI = ClassLoader.getSystemResource("reference/" + nr + "-reference.txt").toURI();
        String reference = Files.readString(Paths.get(referenceURI));

        assertThat(cleanDescription( description.toString() ), is(reference));
    }

    @ParameterizedTest
    @CsvSource({"foo,foo",
            "foo@2ee39e73,foo",
            "some text thing@2ee39e73 and some more,some text thing and some more",
            "some foo@2ee39e73 and some bar@3ab456dc also,some foo and some bar also",
            "leave short@123 alone,leave short@123 alone"
    })
    public void cleanDescriptionTest(String input, String expected) {
        assertThat(cleanDescription(input), is(expected));
    }

    /**
     * Clean a description string by removing all object references ("@abcd1234")
     */
    private String cleanDescription(String description) {
        return description.replaceAll("@[a-fA-F-0-9]{5,}", "");
    }
}
