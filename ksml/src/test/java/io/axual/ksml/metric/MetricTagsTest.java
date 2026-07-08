package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricTagsTest {

    @Test
    void appendReturnsNewInstanceLeavingOriginalUntouched() {
        final var original = new MetricTags();
        final var appended = original.append("app", "ksml");

        assertThat(original).isEmpty();
        assertThat(appended).hasSize(1);
    }

    @Test
    void appendTagAddsEntry() {
        final var tags = new MetricTags().append(new MetricTag("key", "value"));
        assertThat(tags).containsExactly(new MetricTag("key", "value"));
    }

    @Test
    void appendIfPresentAddsWhenValueGiven() {
        assertThat(new MetricTags().appendIfPresent("key", "value")).hasSize(1);
    }

    @Test
    void appendIfPresentIgnoresNullOrBlank() {
        assertThat(new MetricTags().appendIfPresent("key", null)).isEmpty();
        assertThat(new MetricTags().appendIfPresent("key", "  ")).isEmpty();
    }

    @Test
    void toStringJoinsTagsWithComma() {
        final var tags = new MetricTags().append("a", "1").append("b", "2");
        assertThat(tags).hasToString("a=1,b=2");
    }

    @Test
    void emptyTagsRenderAsEmptyString() {
        assertThat(new MetricTags()).hasToString("");
    }
}
