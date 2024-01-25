package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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


import io.axual.ksml.generator.TopologyGeneratorImpl;
import io.axual.ksml.notation.NotationLibrary;
import org.apache.kafka.streams.TopologyDescription;
import org.graalvm.home.Version;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

public class KSMLTopologyGeneratorBasicTest {

    @BeforeAll
    public static void checkGraalVM() {
        if (!Version.getCurrent().isRelease()) {
            fail("This test needs GraalVM to work.");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void parseAndCheckOuput(int nr) throws Exception {
        final URI uri = ClassLoader.getSystemResource("pipelines/" + nr + "-demo.yaml").toURI();
        final Path path = Paths.get(uri);
        String pipeDefinition = Files.readString(path);
        TopologyGeneratorImpl topologyGenerator = new TopologyGeneratorImpl(KSMLConfig.builder()
                .sourceType("content")
                .source(pipeDefinition)
                .notationLibrary(new NotationLibrary(new HashMap<>()))
                .build());

        var streamProperties = new Properties();
        streamProperties.put("bootstrap.servers", "localhost:9092");
        streamProperties.put("application.id", "testing");

        final var topology = topologyGenerator.create("some.app.id", streamProperties);
        final TopologyDescription description = topology.describe();
        System.out.println(description);

        URI referenceURI = ClassLoader.getSystemResource("reference/" + nr + "-reference.txt").toURI();
        String reference = Files.readString(Paths.get(referenceURI));

        assertThat(cleanDescription(description.toString()), is(reference));
    }

    @ParameterizedTest
    @CsvSource({"foo,foo",
            "foo@2ee39e73,foo",
            "some text thing@2ee39e73 and some more,some text thing and some more",
            "some foo@2ee39e73 and some bar@3ab456dc also,some foo and some bar also",
            "leave short@123 alone,leave short@123 alone"
    })
    void cleanDescriptionTest(String input, String expected) {
        System.out.println("input='" + input + "',expected='" + expected + "'");
        assertThat(cleanDescription(input), is(expected));
    }

    @Test
    @DisplayName("cleanDescription also fixes line endings")
    void cleanDescriptionTestLineEnd() {
        assertThat(cleanDescription("fix\r\nnewlines"), is("fix\nnewlines"));
    }

    /**
     * Clean a description string by removing all object references ("@abcd1234") and fixing Windows line endings.
     */
    private String cleanDescription(String description) {
        return description
                .replaceAll("@[a-fA-F-0-9]{5,}", "")
                .replaceAll("\r\n", "\n");
    }
}
