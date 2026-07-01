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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyValueBeansTest {

    @Test
    @DisplayName("add(key, value) wraps the pair into a bean and exposes it via elements()")
    void addKeyValueWrapsBean() {
        final var beans = new KeyValueBeans().add(new DataString("k"), new DataString("v"));

        assertThat(beans.elements()).hasSize(1);
        final var bean = beans.elements().get(0);
        assertThat(bean.key()).isEqualTo(new DataString("k"));
        assertThat(bean.value()).isEqualTo(new DataString("v"));
    }

    @Test
    @DisplayName("add(KeyValueBeans) merges all elements from another collection")
    void addMergesOtherBeans() {
        final var first = new KeyValueBeans().add(new DataString("k1"), new DataString("v1"));
        final var second = new KeyValueBeans()
                .add(new DataString("k2"), new DataString("v2"))
                .add(new DataString("k3"), new DataString("v3"));

        first.add(second);

        assertThat(first.elements()).hasSize(3);
    }

    @Test
    @DisplayName("toString embeds the string representation of the contained elements")
    void toStringContainsElements() {
        final var beans = new KeyValueBeans().add(new DataString("k"), new DataString("v"));

        assertThat(beans.toString()).contains(beans.elements().get(0).toString());
    }
}
