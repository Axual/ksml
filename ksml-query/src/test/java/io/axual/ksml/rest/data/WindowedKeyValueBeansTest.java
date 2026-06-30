package io.axual.ksml.rest.data;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
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

import io.axual.ksml.data.object.DataString;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WindowedKeyValueBeansTest {

    private static TimeWindow window() {
        return new TimeWindow(0L, 100L);
    }

    @Test
    @DisplayName("add(window, key, value) wraps the entry into a windowed bean")
    void addWindowKeyValueWrapsBean() {
        final var beans = new WindowedKeyValueBeans().add(window(), new DataString("k"), new DataString("v"));

        assertThat(beans.elements()).hasSize(1);
        assertThat(beans.elements().get(0).key()).isEqualTo(new DataString("k"));
        assertThat(beans.elements().get(0).window().end()).isEqualTo(100L);
    }

    @Test
    @DisplayName("add(WindowedKeyValueBeans) merges all elements from another collection")
    void addMergesOtherBeans() {
        final var first = new WindowedKeyValueBeans().add(window(), new DataString("k1"), new DataString("v1"));
        final var second = new WindowedKeyValueBeans()
                .add(window(), new DataString("k2"), new DataString("v2"))
                .add(new WindowedKeyValueBean(window(), new DataString("k3"), new DataString("v3")));

        first.add(second);

        assertThat(first.elements()).hasSize(3);
        assertThat(first.toString()).contains("StoreData");
    }
}
