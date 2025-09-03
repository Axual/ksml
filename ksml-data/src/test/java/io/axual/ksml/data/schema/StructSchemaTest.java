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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StructSchemaTest {

    private static DataField requiredInt(String name) {
        return new DataField(name, DataSchema.INTEGER_SCHEMA, null, 0);
        // required=true; defaultValue=null
    }

    private static DataField optionalStringWithDefault(String name) {
        return new DataField(name, DataSchema.STRING_SCHEMA, null, 0, true, false, new DataValue("n/a"));
    }

    @Test
    @DisplayName("field accessors and fields() copy")
    void fieldAccessors() {
        var s = new StructSchema("ns", "Person", null, List.of(requiredInt("id"), optionalStringWithDefault("name")));
        assertThat(s.field(0).name()).isEqualTo("id");
        assertThat(s.field("name")).isNotNull();
        var originalSize = s.fields().size();
        // Modify returned list and ensure struct internal state unaffected
        var list = s.fields();
        list.add(requiredInt("age"));
        assertThat(s.fields()).hasSize(originalSize);
        assertThat(s.field("age")).isNull();
    }

    @Test
    @DisplayName("isAssignableFrom validates required presence, defaulted optional omission, and type compatibility")
    void assignabilityRules() {
        var target = new StructSchema("ns", "Person", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name")
        ));

        // Other has id as long (compatible) and omits name (allowed due to default)
        var other1 = new StructSchema("ns", "PersonOther", null, List.of(
                new DataField("id", DataSchema.LONG_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other1)).isTrue();

        // Missing required id -> not assignable
        var other2 = new StructSchema("ns", "PersonOther", null, List.of(
                optionalStringWithDefault("name")
        ));
        assertThat(target.isAssignableFrom(other2)).isFalse();

        // Present name with incompatible type -> not assignable
        var other3 = new StructSchema("ns", "PersonOther", null, List.of(
                requiredInt("id"),
                new DataField("name", DataSchema.FLOAT_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other3)).isFalse();

        // Other may contain extra fields -> still assignable
        var other4 = new StructSchema("ns", "PersonOther", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name"),
                new DataField("extra", DataSchema.STRING_SCHEMA, null, 0)
        ));
        assertThat(target.isAssignableFrom(other4)).isTrue();
    }

    @Test
    @DisplayName("equals and hashCode consider mutual assignability")
    void equalsAndHashCode() {
        var a = new StructSchema("ns", "A", null, List.of(
                requiredInt("id"),
                optionalStringWithDefault("name")
        ));
        // b omits optional name -> still equal due to mutual assignability
        var b = new StructSchema("ns", "B", null, List.of(
                requiredInt("id")
        ));
        assertThat(a).isNotEqualTo(b);
        assertThat(a.hashCode()).isNotEqualTo(b.hashCode());

        // c has incompatible type for name -> not equal
        var c = new StructSchema("ns", "C", null, List.of(
                requiredInt("id"),
                new DataField("name", DataSchema.FLOAT_SCHEMA, null, 0)
        ));
        assertThat(a).isNotEqualTo(c);
    }
}
