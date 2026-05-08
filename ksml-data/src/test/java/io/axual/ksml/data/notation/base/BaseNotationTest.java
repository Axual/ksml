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

    private static final SimpleType DEFAULT_TYPE = new SimpleType(String.class, "string");
    private static final Notation.Converter CONVERTER = (value, targetType) -> value;
    private static final Notation.SchemaParser PARSER = (ctx, name, schema) -> null;

    private static class DummyNotation extends BaseNotation {
        DummyNotation(NotationContext context) {
            super(context);
        }

        @Override
        public String notationName() {
            return "dummy";
        }

        @Override
        public String filenameExtension() {
            return ".json";
        }

        @Override
        public SchemaUsage schemaUsage() {
            return SchemaUsage.SCHEMALESS_ONLY;
        }

        @Override
        public DataType defaultType() {
            return DEFAULT_TYPE;
        }

        @Override
        public Converter converter() {
            return CONVERTER;
        }

        @Override
        public SchemaParser schemaParser() {
            return PARSER;
        }

        @Override
        public Serde<Object> serde(DataType type, boolean isKey) {
            throw noSerdeFor(type);
        }
    }

    @Test
    @DisplayName("name() delegates to notationName(); method overrides expose constants; noSerdeFor message")
    void baseBehaviourAndNoSerdeFor() {
        var context = new NotationContext();
        var notation = new DummyNotation(context);

        assertThat(notation.name()).isEqualTo("dummy");
        assertThat(notation.notationName()).isEqualTo("dummy");
        assertThat(notation.filenameExtension()).isEqualTo(".json");
        assertThat(notation.defaultType()).isEqualTo(DEFAULT_TYPE);
        assertThat(notation.converter()).isSameAs(CONVERTER);
        assertThat(notation.schemaParser()).isSameAs(PARSER);
        assertThat(notation.context()).isSameAs(context);

        var wrongType = new SimpleType(Integer.class, "int");
        assertThatThrownBy(() -> notation.serde(wrongType, true))
                .hasMessageEndingWith("dummy serde not available for data type: " + wrongType)
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class);
    }
}
