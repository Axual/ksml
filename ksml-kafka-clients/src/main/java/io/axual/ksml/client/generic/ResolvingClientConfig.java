package io.axual.ksml.client.generic;

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

import io.axual.ksml.client.exception.ClientException;
import io.axual.ksml.client.exception.ConfigException;
import io.axual.ksml.client.resolving.GroupPatternResolver;
import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.TopicPatternResolver;
import io.axual.ksml.client.resolving.TopicResolver;
import io.axual.ksml.client.util.FactoryUtil;
import io.axual.ksml.client.util.MapUtil;
import lombok.Getter;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ResolvingClientConfig {
    public static final String GROUP_ID_PATTERN_CONFIG = "group.id.pattern";
    public static final String TOPIC_PATTERN_CONFIG = "topic.pattern";

    protected final Map<String, Object> configs;
    @Getter
    protected final Map<String, Object> downstreamConfigs;
    @Getter
    public final GroupResolver groupResolver;
    @Getter
    public final TopicResolver topicResolver;

    public ResolvingClientConfig(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        downstreamConfigs = new HashMap<>(configs);
        var defaultContext = MapUtil.toStringValues(configs);

        var groupPattern = configs.get(GROUP_ID_PATTERN_CONFIG);
        groupResolver = groupPattern != null
                ? new GroupPatternResolver(groupPattern.toString(), defaultContext)
                : new GroupPatternResolver(GroupPatternResolver.DEFAULT_PLACEHOLDER_PATTERN, defaultContext);

        var topicPattern = configs.get(TOPIC_PATTERN_CONFIG);
        topicResolver = topicPattern != null
                ? new TopicPatternResolver(topicPattern.toString(), defaultContext)
                : new TopicPatternResolver(TopicPatternResolver.DEFAULT_PLACEHOLDER_PATTERN, defaultContext);
    }

    public <T> T getConfiguredInstance(String key, Class<T> expectedClass) {
        return getConfiguredInstance(key, expectedClass, false);
    }

    public <T> T getConfiguredInstance(String key, Class<T> expectedClass, boolean allowNull) {
        final Object configuredValue = configs.get(key);
        try {
            return getConfiguredInstance(configuredValue, expectedClass, allowNull);
        } catch (ConfigException e) {
            throw new ConfigException(key, configuredValue, "Property not set or contains illegal value");
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getConfiguredInstance(Object value, Class<T> expectedClass, boolean allowNull) {
        // Check if the value is an initialized instance of the expected class
        if (expectedClass.isInstance(value)) {
            return (T) value;
        }

        // Check if the value is a string
        if (value instanceof String) {
            // Assume the value represents the object's class name, so return new instance
            T result = FactoryUtil.create((String) value, expectedClass);
            if (result instanceof Configurable configurableResult) {
                configurableResult.configure(downstreamConfigs);
            }
            return result;
        }

        // Check if the value is an instance of Class
        if (value instanceof Class<?> valueClass) {
            // If expectedClass is (superclass of) configuredClass, then create an instance of the
            // configuredClass and return it casted as the expectedClass
            if (expectedClass.isAssignableFrom(valueClass)) {
                T result = Utils.newInstance((valueClass).asSubclass(expectedClass));
                if (result instanceof Configurable configurableResult) {
                    configurableResult.configure(downstreamConfigs);
                }
                return result;
            }
        }

        if (allowNull) {
            return null;
        }

        throw new ClientException("Can not instantiate object of type " + expectedClass.getSimpleName() + " from value " + (value != null ? value.toString() : "null"));
    }
}
