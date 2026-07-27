package io.axual.ksml.client.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import io.axual.ksml.client.exception.ClientException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FactoryUtilTest {
    @Test
    @DisplayName("A valid class name is instantiated as the requested type")
    void createInstantiatesValidClass() {
        assertThat(FactoryUtil.create("java.lang.String", CharSequence.class)).isInstanceOf(String.class);
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    @DisplayName("A missing class name throws a ClientException")
    void createRejectsMissingClassName(String className) {
        assertThatThrownBy(() -> FactoryUtil.create(className, CharSequence.class))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining("No")
                .hasMessageContaining("class passed");
    }

    @Test
    @DisplayName("An unknown class name throws a ClientException with the missing class")
    void createRejectsUnknownClass() {
        assertThatThrownBy(() -> FactoryUtil.create("com.example.DoesNotExist", CharSequence.class))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining("Class not found")
                .hasCauseInstanceOf(ClassNotFoundException.class);
    }

    @Test
    @DisplayName("A class that cannot be cast to the requested type throws a ClientException")
    void createRejectsIncompatibleClass() {
        assertThatThrownBy(() -> FactoryUtil.create("java.lang.String", Integer.class))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining("Could not cast");
    }
}
