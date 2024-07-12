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

import java.util.HashMap;
import java.util.Map;

public class TopicPatternResolver extends CachedPatternResolver implements TopicResolver {
    public static final String DEFAULT_PLACEHOLDER_VALUE = "topic";
    public static final String DEFAULT_PLACEHOLDER_PATTERN = FIELD_NAME_PREFIX + DEFAULT_PLACEHOLDER_VALUE + FIELD_NAME_SUFFIX;
    private static final String INTERNAL_TOPIC_PREFIX = "_";

    public TopicPatternResolver(String topicPattern, Map<String, String> defaultValues) {
        super(topicPattern, DEFAULT_PLACEHOLDER_VALUE, defaultValues);
    }

    @Override
    public String resolve(String name) {
        if (isInternalUnresolvedTopic(name)) return name;
        return super.resolve(name);
    }

    @Override
    public String unresolve(String name) {
        if (isInternalResolvedTopic(name)) return name;
        return super.unresolve(name);
    }

    @Override
    public Map<String, String> unresolveContext(String name) {
        if (isInternalResolvedTopic(name)) {
            var result = new HashMap<String, String>();
            result.put(DEFAULT_PLACEHOLDER_VALUE, name);
            return result;
        }
        return super.unresolveContext(name);
    }

    private boolean isInternalUnresolvedTopic(String name) {
        return isInternalTopic(name);
    }

    private boolean isInternalResolvedTopic(String name) {
        return isInternalTopic(name);
    }

    private boolean isInternalTopic(String name) {
        return name.startsWith(INTERNAL_TOPIC_PREFIX);
    }
}
