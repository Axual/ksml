package io.axual.ksml.data.notation.base;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BaseNotationTest {
    private static class DummyNotation extends BaseNotation {
        DummyNotation(NotationContext context, String fileExt, DataType defaultType,
                      Notation.Converter converter, Notation.SchemaParser parser) {
            super(context, fileExt, defaultType, converter, parser);
        }

        @Override
        public Serde<Object> serde(DataType type, boolean isKey) {
            // Always fail with the standard message to expose BaseNotation.noSerdeFor
            throw noSerdeFor(type);
        }
    }

    @Test
    @DisplayName("name() delegates to context; getters expose provided constructor arguments; noSerdeFor message")
    void baseBehaviourAndNoSerdeFor() {
        var context = new NotationContext("json", "vendorX");
        var defaultType = new SimpleType(String.class, "string");
        Notation.Converter converter = (value, targetType) -> value; // not used
        Notation.SchemaParser parser = (ctx, name, schema) -> null; // not used

        var notation = new DummyNotation(context, ".json", defaultType, converter, parser);

        assertThat(notation.name()).isEqualTo("vendorX_json");
        assertThat(notation.filenameExtension()).isEqualTo(".json");
        assertThat(notation.defaultType()).isEqualTo(defaultType);
        assertThat(notation.converter()).isSameAs(converter);
        assertThat(notation.schemaParser()).isSameAs(parser);

        var wrongType = new SimpleType(Integer.class, "int");
        assertThatThrownBy(() -> notation.serde(wrongType, true))
                .hasMessageEndingWith("vendorX_json serde not available for data type: " + wrongType)
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class);
    }
}
