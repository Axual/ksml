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

import com.codahale.metrics.jmx.ObjectNameFactory;
import io.axual.ksml.data.tag.ContextTag;
import lombok.extern.slf4j.Slf4j;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
class MetricObjectNameFactory implements ObjectNameFactory {
    private static final String TAG_KEY_NAME = MetricObjectNaming.NAME_TAG_KEY;
    private static final String TAG_KEY_TYPE = "type";
    private static final Set<String> INVALID_TAG_KEYS = Set.of(TAG_KEY_NAME, TAG_KEY_TYPE);

    private final List<ContextTag> staticTags;

    MetricObjectNameFactory(List<ContextTag> staticTags) {
        if (staticTags.stream().anyMatch(tag -> tag == null || INVALID_TAG_KEYS.contains(tag.key()))) {
            throw new IllegalArgumentException("Invalid tags provided");
        }
        this.staticTags = Collections.unmodifiableList(staticTags);
    }

    @Override
    public ObjectName createName(String type, String domain, String name) {
        final var metricName = MetricObjectNaming.metricNameFromString(name);
        final var metricTags = metricName.tags();

        final var tagList = new ArrayList<ContextTag>(staticTags.size() + 1 + metricTags.size());
        tagList.add(MetricsUtil.metricTag(TAG_KEY_NAME, metricName.name()));
        tagList.addAll(staticTags);
        tagList.addAll(metricTags);

        // Build the ObjectName string directly to guarantee order of tag keys
        StringBuilder mBeanName = new StringBuilder();
        mBeanName.append(jmxSanitize(domain));
        mBeanName.append(":").append(TAG_KEY_TYPE).append("=");
        mBeanName.append(jmxSanitize(type));
        for (var tag : tagList) {
            mBeanName.append(",");
            mBeanName.append(jmxSanitize(tag.key()));
            mBeanName.append("=");
            mBeanName.append(jmxSanitize(tag.value()));
        }

        try {
            return new ObjectName(mBeanName.toString());
        } catch (MalformedObjectNameException e) {
            log.warn("Could not create object name for name {}, type {}, static tags {} metric tags {}. Using alternative name", metricName.name(), type, staticTags, metricTags, e);

            final var altName = MetricObjectNaming.stringFromMetricsTags(tagList);
            try {
                return new ObjectName(jmxSanitize(domain), "name", ObjectName.quote(altName));
            } catch (MalformedObjectNameException ex) {
                throw new MetricObjectNamingException(ex);
            }
        }
    }

    /**
     * Even though only a small number of characters are disallowed in JMX, quote any
     * string containing special characters to be safe.
     */
    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    /**
     * Quote `name` using {@link ObjectName#quote(String)} if `name` contains
     * characters that are not safe for use in JMX.
     */
    private static String jmxSanitize(String name) {
        return MBEAN_PATTERN.matcher(name).matches() ? name : ObjectName.quote(name);
    }
}
