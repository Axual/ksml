package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.compare.EqualityFlags;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UnionSchemaMemberTest {

    private static final int NO_TAG = -1;

    @Test
    @DisplayName("The convenience constructor defaults name/doc to null and tag to NO_TAG")
    void convenienceConstructorDefaults() {
        final var member = new UnionSchema.Member(DataSchema.INTEGER_SCHEMA);

        assertThat(member.name()).isNull();
        assertThat(member.doc()).isNull();
        assertThat(member.tag()).isEqualTo(NO_TAG);
        assertThat(member.schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
    }

    @Test
    @DisplayName("The full constructor exposes all fields")
    void fullConstructor() {
        final var member = new UnionSchema.Member("name", DataSchema.INTEGER_SCHEMA, "doc", 7);

        assertThat(member.name()).isEqualTo("name");
        assertThat(member.schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(member.doc()).isEqualTo("doc");
        assertThat(member.tag()).isEqualTo(7);
    }

    @Test
    @DisplayName("Deep equals: equal members are equal; identity short-circuits; null/foreign types are not equal")
    void deepEqualsBasics() {
        final var member = new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "d", 1);

        assertThat(member.equals(member, EqualityFlags.EMPTY).isEqual()).isTrue();
        assertThat(member.equals(new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "d", 1), EqualityFlags.EMPTY).isEqual()).isTrue();
        assertThat(member.equals(null, EqualityFlags.EMPTY).isNotEqual()).isTrue();
        assertThat(member.equals("not-a-member", EqualityFlags.EMPTY).isNotEqual()).isTrue();
    }

    @Test
    @DisplayName("Deep equals: a difference in any field makes members not equal")
    void deepEqualsPerField() {
        final var base = new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "d", 1);

        assertThat(base.equals(new UnionSchema.Member("other", DataSchema.INTEGER_SCHEMA, "d", 1), EqualityFlags.EMPTY).isNotEqual()).isTrue();
        assertThat(base.equals(new UnionSchema.Member("n", DataSchema.LONG_SCHEMA, "d", 1), EqualityFlags.EMPTY).isNotEqual()).isTrue();
        assertThat(base.equals(new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "other", 1), EqualityFlags.EMPTY).isNotEqual()).isTrue();
        assertThat(base.equals(new UnionSchema.Member("n", DataSchema.INTEGER_SCHEMA, "d", 99), EqualityFlags.EMPTY).isNotEqual()).isTrue();
    }
}
