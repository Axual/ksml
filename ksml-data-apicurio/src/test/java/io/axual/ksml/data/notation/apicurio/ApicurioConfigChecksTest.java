package io.axual.ksml.data.notation.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - Apicurio common
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

import io.axual.ksml.data.exception.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioConfigChecksTest {

    @Test
    @DisplayName("A null or empty config passes")
    void acceptsNullAndEmpty() {
        assertThatCode(() -> ApicurioConfigChecks.rejectV2Configs(null)).doesNotThrowAnyException();
        assertThatCode(() -> ApicurioConfigChecks.rejectV2Configs(Map.of())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Valid v3 settings pass")
    void acceptsV3Configs() {
        final var configs = Map.of(
                "apicurio.registry.url", "http://registry:8081/apis/registry/v3",
                "apicurio.registry.auth.username", "alice",
                "apicurio.registry.id-handler", "io.apicurio.registry.serde.Default4ByteIdHandler");

        assertThatCode(() -> ApicurioConfigChecks.rejectV2Configs(configs)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("The renamed auth keys are rejected and name their replacement")
    void rejectsRenamedAuthKeys() {
        final var withUsername = Map.of("apicurio.auth.username", "alice");
        final var withPassword = Map.of("apicurio.auth.password", "secret");

        assertThatThrownBy(() -> ApicurioConfigChecks.rejectV2Configs(withUsername))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("apicurio.auth.username")
                .hasMessageContaining("apicurio.registry.auth.username");

        assertThatThrownBy(() -> ApicurioConfigChecks.rejectV2Configs(withPassword))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("apicurio.auth.password")
                .hasMessageContaining("apicurio.registry.auth.password");
    }

    @Test
    @DisplayName("The removed as-confluent key is rejected and points at the id settings")
    void rejectsRemovedAsConfluentKey() {
        final var configs = Map.of("apicurio.registry.as-confluent", "true");

        assertThatThrownBy(() -> ApicurioConfigChecks.rejectV2Configs(configs))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("apicurio.registry.as-confluent")
                .hasMessageContaining("apicurio.registry.id-handler")
                .hasMessageContaining("apicurio.registry.use-id");
    }

    @Test
    @DisplayName("The removed Legacy4ByteIdHandler value is rejected and names the new class")
    void rejectsRemovedIdHandlerValue() {
        final var configs = Map.of("apicurio.registry.id-handler", ApicurioConfigChecks.LEGACY_4_BYTE_ID_HANDLER);

        assertThatThrownBy(() -> ApicurioConfigChecks.rejectV2Configs(configs))
                .isInstanceOf(DataException.class)
                .hasMessageContaining(ApicurioConfigChecks.LEGACY_4_BYTE_ID_HANDLER)
                .hasMessageContaining("Default4ByteIdHandler");
    }

    @Test
    @DisplayName("A null value for the id-handler key is not mistaken for the removed class")
    void toleratesNullIdHandlerValue() {
        final var configs = new HashMap<String, Object>();
        configs.put("apicurio.registry.id-handler", null);

        assertThatCode(() -> ApicurioConfigChecks.rejectV2Configs(configs)).doesNotThrowAnyException();
    }
}
