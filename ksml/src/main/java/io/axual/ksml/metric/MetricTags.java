package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import lombok.EqualsAndHashCode;

import java.util.ArrayList;

@EqualsAndHashCode
public class MetricTags extends ArrayList<MetricTag> {
    public MetricTags append(MetricTag tag) {
        final var result = new MetricTags();
        result.addAll(this);
        result.add(tag);
        return result;
    }

    public MetricTags append(String key, String value) {
        return append(new MetricTag(key, value));
    }

    @Override
    public String toString() {
        final var result = new StringBuilder();
        forEach(t -> result.append(!result.isEmpty() ? "," : "").append(t));
        return result.toString();
    }
}
