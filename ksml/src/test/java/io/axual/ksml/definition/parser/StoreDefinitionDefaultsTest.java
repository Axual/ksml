package io.axual.ksml.definition.parser;

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

import tools.jackson.databind.JsonNode;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.type.UserType;
import io.axual.ksml.generator.YAMLObjectMapper;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Declared stores get the Kafka Streams defaults: an option the definition leaves out is {@code true} for
 * {@code persistent}, {@code logging} and {@code timestamped}. An option set explicitly keeps its value.
 * The definitions are parsed through the real parsers, so the tests cover what a KSML user writes.
 */
class StoreDefinitionDefaultsTest {

    @BeforeAll
    static void registerNotations() {
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);
    }

    private static ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    @Test
    @DisplayName("a keyValue store without options is persistent, timestamped and logged")
    void keyValueStoreWithoutOptions() throws Exception {
        final var store = new KeyValueStateStoreDefinitionParser(false, true).parser()
                .parse(nodeOf("name: kv_store\nkeyType: string\nvalueType: string"));

        assertThat(store.persistent()).isTrue();
        assertThat(store.timestamped()).isTrue();
        assertThat(store.logging()).isTrue();

        final var builder = StoreUtil.getStoreBuilder(store);
        assertThat(builder.loggingEnabled()).isTrue();
        final var built = builder.build();
        assertThat(built.persistent()).isTrue();
        assertThat(built).isInstanceOf(TimestampedKeyValueStore.class);
    }

    @Test
    @DisplayName("a window store without options is persistent, timestamped and logged")
    void windowStoreWithoutOptions() throws Exception {
        final var store = new WindowStateStoreDefinitionParser(false, true).parser()
                .parse(nodeOf("name: window_store\nkeyType: string\nvalueType: string\nwindowSize: 10s\nretention: 1m"));

        assertThat(store.persistent()).isTrue();
        assertThat(store.timestamped()).isTrue();
        assertThat(store.logging()).isTrue();

        assertThat(StoreUtil.getStoreBuilder(store).loggingEnabled()).isTrue();
        final var materialized = new MaterializedInternal<>(StoreUtil.materialize(store).materialized());
        assertThat(materialized.loggingEnabled()).isTrue();
        final var supplied = materialized.storeSupplier().get();
        assertThat(supplied.persistent()).isTrue();
        assertThat(supplied).isInstanceOf(TimestampedBytesStore.class);
    }

    @Test
    @DisplayName("a session store without options is persistent and logged")
    void sessionStoreWithoutOptions() throws Exception {
        final var store = new SessionStateStoreDefinitionParser(false, true).parser()
                .parse(nodeOf("name: session_store\nkeyType: string\nvalueType: string\nretention: 1m"));

        assertThat(store.persistent()).isTrue();
        assertThat(store.logging()).isTrue();

        final var builder = StoreUtil.getStoreBuilder(store);
        assertThat(builder.loggingEnabled()).isTrue();
        assertThat(builder.build().persistent()).isTrue();
    }

    @Test
    @DisplayName("explicit false values are kept")
    void explicitFalseIsKept() throws Exception {
        final var store = new WindowStateStoreDefinitionParser(false, true).parser()
                .parse(nodeOf("name: window_store\nkeyType: string\nvalueType: string\nwindowSize: 10s\nretention: 1m\npersistent: false\ntimestamped: false\nlogging: false"));

        assertThat(store.persistent()).isFalse();
        assertThat(store.timestamped()).isFalse();
        assertThat(store.logging()).isFalse();

        final var builder = StoreUtil.getStoreBuilder(store);
        assertThat(builder.loggingEnabled()).isFalse();
        assertThat(builder.build().persistent()).isFalse();
    }

    @Test
    @DisplayName("a versioned keyValue store without persistent is a persistent, logged versioned store")
    void versionedStoreWithoutPersistent() throws Exception {
        final var store = new KeyValueStateStoreDefinitionParser(false, true).parser()
                .parse(nodeOf("name: versioned_store\nkeyType: string\nvalueType: string\nversioned: true\nhistoryRetention: 1h"));

        final var builder = StoreUtil.getStoreBuilder(store);
        assertThat(builder.loggingEnabled()).isTrue();
        final var built = builder.build();
        assertThat(built.persistent()).isTrue();
        assertThat(built).isInstanceOf(VersionedKeyValueStore.class);
    }
}
