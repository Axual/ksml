package io.axual.ksml.data.compare;

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

import io.axual.ksml.data.object.DataObjectFlag;
import io.axual.ksml.data.schema.DataSchemaFlag;
import io.axual.ksml.data.type.DataTypeFlag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class EqualityFlagsTest {

    @Test
    @DisplayName("An empty instance has no flags set")
    void emptyHasNothingSet() {
        final var flags = new EqualityFlags();

        assertThat(flags.isSet(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE)).isFalse();
        assertThat(flags.getAll()).isEmpty();
        assertThat(flags.isSet(null)).isFalse();
        assertThat(EqualityFlags.EMPTY.getAll()).isEmpty();
    }

    @Test
    @DisplayName("Varargs constructor routes each flag to its category and isSet reflects it")
    void varargsConstructorRoutesFlags() {
        final var flags = new EqualityFlags(
                DataObjectFlag.IGNORE_DATA_TUPLE_TYPE,
                DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_NAME,
                DataSchemaFlag.IGNORE_UNION_SCHEMA_MEMBER_NAME);

        assertThat(flags.isSet(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE)).isTrue();
        assertThat(flags.isSet(DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_NAME)).isTrue();
        assertThat(flags.isSet(DataSchemaFlag.IGNORE_UNION_SCHEMA_MEMBER_NAME)).isTrue();
        assertThat(flags.isSet(DataObjectFlag.IGNORE_DATA_MAP_TYPE)).isFalse();
        assertThat(flags.getAll()).hasSize(3);
    }

    @Test
    @DisplayName("Set constructor initialises from the provided flags")
    void setConstructor() {
        final var flags = new EqualityFlags(Set.of(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE));

        assertThat(flags.isSet(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE)).isTrue();
    }

    @Test
    @DisplayName("Copy constructor keeps existing flags and adds new ones")
    void copyConstructorAddsFlags() {
        final var base = new EqualityFlags(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE);

        final var extended = new EqualityFlags(base, DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_TAG);

        assertThat(extended.isSet(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE)).isTrue();
        assertThat(extended.isSet(DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_TAG)).isTrue();
    }

    @Test
    @DisplayName("ifSetThenAdd only extends the flags when the queried flag is set")
    void ifSetThenAdd() {
        final var base = new EqualityFlags(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE);

        final var added = base.ifSetThenAdd(DataObjectFlag.IGNORE_DATA_TUPLE_TYPE, DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_TAG);
        assertThat(added.isSet(DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_TAG)).isTrue();

        // When the queried flag is not set, the same instance is returned unchanged.
        final var unchanged = base.ifSetThenAdd(DataObjectFlag.IGNORE_DATA_MAP_TYPE, DataTypeFlag.IGNORE_UNION_TYPE_MEMBER_TAG);
        assertThat(unchanged).isSameAs(base);
    }
}
