package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

@Slf4j
class PatternResolverTest {
    private static final String TOPIC = "topic";
    private static final String TENANT = "tenant";
    private static final String INSTANCE = "instance";
    private static final String ENVIRONMENT = "environment";

    private static final String[] PATTERNS = {
            "{tenant}-{instance}-{environment}-{topic}",
            "{tenant}-{instance}-{topic}-{environment}",
//            "{tenant}##${instance}$#-#{environment}#$$#---#{topic}"
    };

    @Test
    @DisplayName("Test converter patterns")
    void testConverter() {
        var vars = new HashMap<String, String>();
        vars.put(TOPIC, "topicname");
        vars.put(TENANT, "tenantnane");
        vars.put(INSTANCE, "instancename");
        vars.put(ENVIRONMENT, "envname");
        for (String pattern : PATTERNS) {
            var converter = new PatternResolver(pattern, TOPIC, vars);
            var composed = converter.resolve("topicname");
            var unresolved = converter.unresolveContext(composed);
            assert (vars.equals(unresolved));
        }
    }
}
