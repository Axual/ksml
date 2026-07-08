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

class MetricTagTest {

    @Test
    void rejectsNullOrBlankKey() {
        assertThatThrownBy(() -> new MetricTag(null, "v")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetricTag(" ", "v")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullOrBlankValue() {
        assertThatThrownBy(() -> new MetricTag("k", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetricTag("k", " ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringRendersKeyValue() {
        assertThat(new MetricTag("app", "ksml")).hasToString("app=ksml");
    }
}
