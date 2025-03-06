package io.axual.ksml.client.admin;

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

import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ResolverUtil {
    private ResolverUtil() {
    }

    public static AclBindingFilter resolve(AclBindingFilter filter, TopicResolver topicResolver, GroupResolver groupResolver) {
        var name = filter.patternFilter().name();
        if (filter.patternFilter().resourceType() == ResourceType.GROUP) name = groupResolver.resolve(name);
        if (filter.patternFilter().resourceType() == ResourceType.TOPIC) name = topicResolver.resolve(name);

        return new AclBindingFilter(
                new ResourcePatternFilter(filter.patternFilter().resourceType(), name, filter.patternFilter().patternType()),
                filter.entryFilter());
    }

    public static Collection<AclBinding> unresolveKeys(Collection<AclBinding> bindings, final TopicResolver topicResolver, final GroupResolver groupResolver) {
        Collection<AclBinding> result = new ArrayList<>(bindings.size());
        for (AclBinding binding : bindings) {
            var name = binding.pattern().name();
            if (binding.pattern().resourceType() == ResourceType.GROUP) name = groupResolver.unresolve(name);
            if (binding.pattern().resourceType() == ResourceType.TOPIC) name = topicResolver.unresolve(name);

            result.add(new AclBinding(
                    new ResourcePattern(binding.pattern().resourceType(), name, binding.pattern().patternType()),
                    binding.entry()));
        }

        return result;
    }

    public static <T> Map<String, T> unresolveKeys(Map<String, T> map, TopicResolver resolver) {
        final var result = new HashMap<String, T>();
        map.forEach((key, value) -> result.put(resolver.unresolve(key), value));
        return result;
    }
}
