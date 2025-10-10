package io.axual.ksml.data.type;

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

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.value.Tuple;

import static org.assertj.core.api.Assertions.assertThat;

class TupleTypeTest {

    @Test
    @DisplayName("Constructor builds name/spec from subtypes and uses Tuple as container class")
    void constructorProperties() {
        var t = new TupleType(io.axual.ksml.data.object.DataString.DATATYPE, io.axual.ksml.data.object.DataInteger.DATATYPE);
        assertThat(t)
                .returns(Tuple.class, TupleType::containerClass)
                .returns("TupleOfStringAndInteger", TupleType::name)
                .returns("(string, integer)", TupleType::spec)
                .hasToString("TupleOfStringAndInteger");
        assertThat(t.subTypeCount()).isEqualTo(2);
        assertThat(t.subType(0)).isEqualTo(io.axual.ksml.data.object.DataString.DATATYPE);
        assertThat(t.subType(1)).isEqualTo(io.axual.ksml.data.object.DataInteger.DATATYPE);
    }

    @Test
    @DisplayName("Assignability compares each position using ComplexType logic")
    void assignabilityBetweenTuples() {
        var numberString = new TupleType(new SimpleType(Number.class, "number"), new SimpleType(String.class, "string"));
        var integerString = new TupleType(new SimpleType(Integer.class, "integer"), new SimpleType(String.class, "string"));
        assertThat(numberString.isAssignableFrom(integerString).isAssignable()).isTrue();
        assertThat(integerString.isAssignableFrom(numberString).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("Assignability from Class checks Tuple.class")
    void assignabilityFromClass() {
        var anyTuple = new TupleType(DataType.UNKNOWN, DataType.UNKNOWN);
        assertThat(anyTuple.isAssignableFrom(Tuple.class).isAssignable()).isTrue();
        assertThat(anyTuple.isAssignableFrom(Object.class).isAssignable()).isFalse();
    }

    @Test
    @DisplayName("equals uses mutual assignability; hashCode consistent per instance")
    void equalsAndHashCode() {
        var a1 = new TupleType(DataType.UNKNOWN, DataType.UNKNOWN);
        var a2 = new TupleType(DataType.UNKNOWN, DataType.UNKNOWN);
        var b = new TupleType(new SimpleType(Integer.class, "integer"), new SimpleType(String.class, "string"));

        var softly = new SoftAssertions();
        softly.assertThat(a1.equals(a1)).isTrue();
        softly.assertThat(a1).isEqualTo(a2);
        softly.assertThat(a1).isNotEqualTo(b);
        softly.assertThat(a1.hashCode()).isEqualTo(a1.hashCode());
        softly.assertThat(b.hashCode()).isEqualTo(b.hashCode());
        softly.assertAll();
    }
}
