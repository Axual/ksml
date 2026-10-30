package io.axual.ksml.generator;

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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.jsonschema.JsonSchemaNotation;
import io.axual.ksml.data.notation.jsonschema.confluent.ConfluentJsonSchemaNotationProvider;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamDataTypeTest {
    private static final String TOPIC = "orders";
    private static final StructSchema ORDER_SCHEMA = new StructSchema("io.axual.test", "Order", null,
            List.of(new StructSchema.Field("amount", DataSchema.INTEGER_SCHEMA)));

    @BeforeAll
    static void registerJsonSchemaNotation() {
        // A JSON Schema notation with a mock Schema Registry
        final var context = new NotationContext(Map.of("schema.registry.url", "mock://stream-data-type-test", "auto.register.schemas", "true"));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonSchemaNotation.NOTATION_NAME,
                new ConfluentJsonSchemaNotationProvider().createNotation(context));
    }

    @Test
    @DisplayName("A tombstone stays a tombstone on a JSON Schema topic, which writes null as a value by itself")
    void tombstoneOnJsonSchemaTopic() {
        final var serde = new StreamDataType(new UserType(JsonSchemaNotation.NOTATION_NAME, new StructType(ORDER_SCHEMA)), false).serde();

        assertThat(serde.serializer().serialize(TOPIC, null)).isNull();
        assertThat(serde.deserializer().deserialize(TOPIC, null)).isNull();
    }
}
