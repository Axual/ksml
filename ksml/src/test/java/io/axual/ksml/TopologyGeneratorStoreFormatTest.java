package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** TopologyGenerator builds declared stores in the headers-aware format only when it is asked to (AXPD-12445). */
class TopologyGeneratorStoreFormatTest {

    @BeforeAll
    static void registerNotations() {
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);
    }

    private static TopologyTestDriver driverFor(boolean headersStoreFormat) throws Exception {
        final var path = Paths.get(ClassLoader.getSystemResource("pipelines/test-headers-store-format.yaml").toURI());
        final var yaml = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definition = new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(yaml, "store_format"));
        final var generator = new TopologyGenerator("store_format.app", null, null, headersStoreFormat);
        return new TopologyTestDriver(generator.create(new StreamsBuilder(), Map.of("definition", definition)));
    }

    @Test
    @DisplayName("with the headers store format, a declared store is headers-aware")
    void headersStoreFormat() throws Exception {
        try (var driver = driverFor(true)) {
            assertThat(driver.getAllStateStores().get("count_store")).isInstanceOf(TimestampedKeyValueStoreWithHeaders.class);
        }
    }

    @Test
    @DisplayName("without the headers store format, a declared store is not headers-aware")
    void defaultStoreFormat() throws Exception {
        try (var driver = driverFor(false)) {
            assertThat(driver.getAllStateStores().get("count_store")).isNotInstanceOf(TimestampedKeyValueStoreWithHeaders.class);
        }
    }
}
