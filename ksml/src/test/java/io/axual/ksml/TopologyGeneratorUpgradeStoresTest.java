package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The stores that TopologyGenerator gives to the KSML 1.x upgrade check (AXPD-12445). */
class TopologyGeneratorUpgradeStoresTest {

    @BeforeAll
    static void registerNotations() {
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);
    }

    @Test
    @DisplayName("the upgrade check gets the declared stores without the stream-stream join stores, with their declared values")
    void upgradeCheckedStores() throws Exception {
        final var path = Paths.get(ClassLoader.getSystemResource("pipelines/test-upgrade-checked-stores.yaml").toURI());
        final var yaml = YAMLObjectMapper.INSTANCE.readValue(Files.readString(path), JsonNode.class);
        final var definition = new TopologyDefinitionParser("test").parse(ParseNode.fromRoot(yaml, "upgrade_stores"));
        final var generator = new TopologyGenerator("upgrade_stores.app");

        generator.create(new StreamsBuilder(), Map.of("definition", definition));

        assertThat(generator.upgradeCheckedStores()).extracting(StateStoreDefinition::name)
                .containsExactlyInAnyOrder("python_store", "count_store");
        assertThat(generator.userStateStores()).extracting(StateStoreDefinition::name).containsExactly("python_store");
        // The count store is a copy made by the operation, which keeps the values as declared
        assertThat(generator.upgradeCheckedStores()).allSatisfy(store -> {
            assertThat(store.declaredLogging()).isNull();
            assertThat(store.declaredTimestamped()).isNull();
        });
    }
}
