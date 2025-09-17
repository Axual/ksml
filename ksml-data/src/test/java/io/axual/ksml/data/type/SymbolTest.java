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

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

class SymbolTest {

    @Test
    @DisplayName("Default constructor sets doc=null and tag=NO_TAG")
    void defaultConstructor() {
        var s = new Symbol("A");
        assertThat(s)
                .returns("A", Symbol::name)
                .returns(null, Symbol::doc)
                .returns(NO_TAG, Symbol::tag);
        assertThat(s.hasDoc()).isFalse();
    }

    @Test
    @DisplayName("hasDoc is true only for non-empty doc")
    void hasDocBehavior() {
        var softly = new SoftAssertions();
        softly.assertThat(new Symbol("A", null, NO_TAG).hasDoc()).isFalse();
        softly.assertThat(new Symbol("A", "", NO_TAG).hasDoc()).isFalse();
        softly.assertThat(new Symbol("A", "desc", NO_TAG).hasDoc()).isTrue();
        softly.assertAll();
    }

    @Test
    @DisplayName("isAssignableFrom requires same name and compatible tags (NO_TAG is wildcard)")
    void isAssignableFromBehavior() {
        var aNoTag = new Symbol("A", null, NO_TAG);
        var aNoTag2 = new Symbol("A", null, NO_TAG);
        var a7 = new Symbol("A", null, 7);
        var a8 = new Symbol("A", null, 8);
        var bNoTag = new Symbol("B", null, NO_TAG);

        // Same name, any NO_TAG acts as wildcard
        assertThat(aNoTag.isAssignableFrom(aNoTag2)).isTrue();
        assertThat(aNoTag.isAssignableFrom(a7)).isTrue();
        assertThat(a7.isAssignableFrom(aNoTag)).isTrue();

        // Same name, equal non-NO_TAG -> true; different tags -> false
        assertThat(a7.isAssignableFrom(new Symbol("A", null, 7))).isTrue();
        assertThat(a7.isAssignableFrom(a8)).isFalse();

        // Different names -> false regardless of tags
        assertThat(aNoTag.isAssignableFrom(bNoTag)).isFalse();
        assertThat(a7.isAssignableFrom(new Symbol("B", null, 7))).isFalse();
    }
}
