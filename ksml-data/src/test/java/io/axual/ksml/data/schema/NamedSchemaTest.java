package io.axual.ksml.data.schema;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NamedSchemaTest {

    static class TestNamedSchema extends NamedSchema {
        public TestNamedSchema(String type, String namespace, String name, String doc) {
            super(type, namespace, name, doc);
        }
    }

    @Test
    @DisplayName("hasName/name, hasDoc, fullName and toString behaviors")
    void basicBehaviors() {
        final var unnamed = new TestNamedSchema(DataSchemaConstants.STRUCT_TYPE, "ns", null, null);
        assertThat(unnamed.hasName()).isFalse();
        assertThat(unnamed.name()).isEqualTo("AnonymousTestNamedSchema");
        assertThat(unnamed.hasDoc()).isFalse();

        final var named = new TestNamedSchema(DataSchemaConstants.STRUCT_TYPE, "ns", "Person", "doc");
        assertThat(named.hasName()).isTrue();
        assertThat(named.hasDoc()).isTrue();
        assertThat(named.fullName()).isEqualTo("ns.Person");
        assertThat(named).hasToString("ns.Person");
    }

    @Test
    @DisplayName("checkAssignableFrom ignores namespace/name/doc but requires NamedSchema and same type")
    void checkAssignableFromRules() {
        final var base = new TestNamedSchema(DataSchemaConstants.STRUCT_TYPE, "ns1", "A", "doc1");
        // StructSchema is a NamedSchema with same type "struct"
        final var struct = new StructSchema("ns2", "B", "doc2", null);
        assertThat(base.checkAssignableFrom(struct).isOK()).isTrue();

        // Different type not assignable
        final var enumNamed = new TestNamedSchema(DataSchemaConstants.ENUM_TYPE, "ns", "E", null);
        assertThat(enumNamed.checkAssignableFrom(struct).isOK()).isFalse();

        // Non-named different base type not assignable
        assertThat(base.checkAssignableFrom(DataSchema.STRING_SCHEMA).isOK()).isFalse();
    }
}
