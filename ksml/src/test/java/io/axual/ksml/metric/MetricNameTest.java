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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricNameTest {

    @Test
    void rejectsNullOrBlankName() {
        assertThatThrownBy(() -> new MetricName(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetricName("  ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringWithoutTagsOmitsSeparator() {
        assertThat(new MetricName("latency")).hasToString("name=latency");
    }

    @Test
    void toStringWithTagsAppendsThem() {
        final var tags = new MetricTags().append("app", "ksml");
        assertThat(new MetricName("latency", tags)).hasToString("name=latency,app=ksml");
    }
}
