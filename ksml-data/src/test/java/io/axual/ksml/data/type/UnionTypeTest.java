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
        SimpleType intType = new SimpleType(Integer.class, "integer");
        SimpleType strType = new SimpleType(String.class, "string");
        UnionType.MemberType m1 = new UnionType.MemberType("i", intType, 10);
        UnionType.MemberType m2 = new UnionType.MemberType("s", strType, 20);
        UnionType u = new UnionType(m1, m2);

        assertThat(u)
                .returns(Object.class, UnionType::containerClass)
                .returns("UnionOfIntegerOrString", UnionType::name)
                .returns("union(integer, string)", UnionType::spec)
                .hasToString("UnionOfIntegerOrString");
        assertThat(u.subTypeCount()).isEqualTo(2);
        assertThat(u.subType(0)).isEqualTo(intType);
        assertThat(u.subType(1)).isEqualTo(strType);
        assertThat(u.memberTypes()).containsExactly(m1, m2);
        assertThat(u.memberTypes()[0].tag()).isEqualTo(10);
        assertThat(u.memberTypes()[1].tag()).isEqualTo(20);
    }

    @Test
    @DisplayName("isAssignableFrom: Union from member type, from other union (same order), and from values")
    void isAssignableFromBehavior() {
        SimpleType intType = new SimpleType(Integer.class, "integer");
        SimpleType strType = new SimpleType(String.class, "string");
        UnionType u = new UnionType(new UnionType.MemberType("i", intType, 1), new UnionType.MemberType("s", strType, 2));

        // From member type
        assertThat(u.isAssignableFrom(intType)).isTrue();
        assertThat(u.isAssignableFrom(strType)).isTrue();
        assertThat(u.isAssignableFrom(new SimpleType(Double.class, "double"))).isFalse();

        // From other union: types match, tags may differ, order must match
        UnionType sameOrderDifferentTags = new UnionType(
                new UnionType.MemberType("i", intType, 99), new UnionType.MemberType("s", strType, 100));
        assertThat(u.isAssignableFrom(sameOrderDifferentTags)).isTrue();

        // Different order should not be assignable as union-to-union
        UnionType swapped = new UnionType(
                new UnionType.MemberType("s", strType, 2), new UnionType.MemberType("i", intType, 1));
        assertThat(u.isAssignableFrom(swapped)).isFalse();

        // From Object values
        assertThat(u.isAssignableFrom(123)).isTrue();
        assertThat(u.isAssignableFrom("abc")).isTrue();
        // Per current DataType default behavior, null is accepted by memberType checks
        assertThat(u.isAssignableFrom((Object) null)).isTrue();
    }

    @Test
    @DisplayName("equals uses mutual assignability and ignores tags; order matters; hashCode consistent per instance")
    void equalsAndHashCode() {
        SimpleType intType = new SimpleType(Integer.class, "integer");
        SimpleType strType = new SimpleType(String.class, "string");
        UnionType a = new UnionType(new UnionType.MemberType("i", intType, 1), new UnionType.MemberType("s", strType, 2));
        UnionType aDifferentTags = new UnionType(new UnionType.MemberType("i", intType, 10), new UnionType.MemberType("s", strType, 20));
        UnionType bSwapped = new UnionType(new UnionType.MemberType("s", strType, 2), new UnionType.MemberType("i", intType, 1));
        UnionType cWider = new UnionType(new UnionType.MemberType("i", new SimpleType(Number.class, "number"), 1), new UnionType.MemberType("s", strType, 2));

        SoftAssertions softly = new SoftAssertions();
        // Reflexivity
        softly.assertThat(a.equals(a)).isTrue();
        // Same types, same order, tags differ -> equals true
        softly.assertThat(a).isEqualTo(aDifferentTags);
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
