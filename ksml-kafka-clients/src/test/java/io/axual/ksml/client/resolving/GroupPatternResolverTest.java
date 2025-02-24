package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GroupPatternResolverTest {

    public static final String PATTERN_WITH_DEFAULT_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group.id}";
    public static final String PATTERN_WITH_ALIAS_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group}";
    public static final String TENANT = "kmsl";
    public static final String INSTANCE = "testing";
    public static final String ENVIRONMENT = "test";

    public static final String GROUP_ID = "some.testing-group";
    public static final String RESOLVED_GROUP_ID = "%s-%s-%s-%s".formatted(TENANT, INSTANCE, ENVIRONMENT, GROUP_ID);

    public static final Map<String, String> BASE_CONFIG = Map.of(
            "tenant", TENANT,
            "instance", INSTANCE,
            "environment", ENVIRONMENT);

    @ParameterizedTest
    @DisplayName("Resolve groups")
    @ValueSource(strings = {PATTERN_WITH_DEFAULT_PLACEHOLDER, PATTERN_WITH_ALIAS_PLACEHOLDER})
    void resolveValidPatternAndData(String pattern) {
        var resolver = new GroupPatternResolver(pattern, BASE_CONFIG);
        var resolved = resolver.resolve(GROUP_ID);
        assertEquals(RESOLVED_GROUP_ID, resolved);

        var unresolved = resolver.unresolve(RESOLVED_GROUP_ID);
        assertEquals(GROUP_ID, unresolved);
    }

    @ParameterizedTest
    @DisplayName("Pattern null causes exception")
    @NullSource
    @ValueSource(strings = {"", "nothing-here", "{group.id"})
    void nullPatternException(String pattern) {
        assertThrows(IllegalArgumentException.class, () -> new GroupPatternResolver(pattern, BASE_CONFIG));
    }

}
