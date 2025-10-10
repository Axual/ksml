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

import static org.assertj.core.api.Assertions.assertThat;

class UnionTypeTest {

    @Test
    @DisplayName("Constructor builds name/spec from member types; retains tags; container is Object")
    void constructorProperties() {
        var intType = new SimpleType(Integer.class, "integer");
        var strType = new SimpleType(String.class, "string");
        var m1 = new UnionType.Member("i", intType, 10);
        var m2 = new UnionType.Member("s", strType, 20);
        var u = new UnionType(m1, m2);

        assertThat(u)
                .returns(Object.class, UnionType::containerClass)
                .returns("UnionOfIntegerOrString", UnionType::name)
                .returns("union(integer, string)", UnionType::spec)
                .hasToString("UnionOfIntegerOrString");
        assertThat(u.subTypeCount()).isEqualTo(2);
        assertThat(u.subType(0)).isEqualTo(intType);
        assertThat(u.subType(1)).isEqualTo(strType);
        assertThat(u.members()).containsExactly(m1, m2);
        assertThat(u.members()[0].tag()).isEqualTo(10);
        assertThat(u.members()[1].tag()).isEqualTo(20);
    }

    @Test
    @DisplayName("isAssignableFrom: Union from member type, from other union (ignore order), and from values")
    void isAssignableFromBehavior() {
        var intType = new SimpleType(Integer.class, "integer");
        var strType = new SimpleType(String.class, "string");
        var u = new UnionType(new UnionType.Member("i", intType, 1), new UnionType.Member("s", strType, 2));

        // From member type
        assertThat(u.isAssignableFrom(intType).isAssignable()).isTrue();
        assertThat(u.isAssignableFrom(strType).isAssignable()).isTrue();
        assertThat(u.isAssignableFrom(new SimpleType(Double.class, "double")).isAssignable()).isFalse();

        // From other union: types match, tags may differ, order must match
        var sameOrderDifferentTags = new UnionType(
                new UnionType.Member("i", intType, 99), new UnionType.Member("s", strType, 100));
        assertThat(u.isAssignableFrom(sameOrderDifferentTags).isAssignable()).isTrue();

        // Different order should still be assignable as union-to-union
        var swapped = new UnionType(
                new UnionType.Member("s", strType, 2), new UnionType.Member("i", intType, 1));
        assertThat(u.isAssignableFrom(swapped).isAssignable()).isTrue();

        // From Object values
        assertThat(u.isAssignableFrom(123).isAssignable()).isTrue();
        assertThat(u.isAssignableFrom("abc").isAssignable()).isTrue();
        // Per current DataType default behavior, null is accepted by memberType checks
        assertThat(u.isAssignableFrom((Object) null).isAssignable()).isTrue();
    }

    @Test
    @DisplayName("Check mutual assignability (ignores tags); inequality (tags matter); order matters; hashCode consistent per instance")
    void equalsAndHashCode() {
        var intType = new SimpleType(Integer.class, "integer");
        var strType = new SimpleType(String.class, "string");
        var a = new UnionType(new UnionType.Member("i", intType, 1), new UnionType.Member("s", strType, 2));
        var aDifferentTags = new UnionType(new UnionType.Member("i", intType, 10), new UnionType.Member("s", strType, 20));
        var bSwapped = new UnionType(new UnionType.Member("s", strType, 2), new UnionType.Member("i", intType, 1));
        var cWider = new UnionType(new UnionType.Member("i", new SimpleType(Number.class, "number"), 1), new UnionType.Member("s", strType, 2));

        var softly = new SoftAssertions();
        // Reflexivity
        softly.assertThat(a.equals(a)).isTrue();

        // Same types, same order, tags differ -> assignable true
        softly.assertThat(a.isAssignableFrom(aDifferentTags).isAssignable()).isTrue();
        // Same types, same order, tags differ -> assignable true
        softly.assertThat(aDifferentTags.isAssignableFrom(a).isAssignable()).isTrue();

        // Same types, same order, tags differ -> equals false
        softly.assertThat(a).isNotEqualTo(aDifferentTags);
        // Order matters -> not equal
        softly.assertThat(a).isNotEqualTo(bSwapped);
        // Not mutually assignable -> not equal
        softly.assertThat(a).isNotEqualTo(cWider);
        // hashCode consistent per instance
        softly.assertThat(a.hashCode()).isEqualTo(a.hashCode());
        softly.assertThat(aDifferentTags.hashCode()).isEqualTo(aDifferentTags.hashCode());
        softly.assertAll();
    }
}
