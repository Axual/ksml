package io.axual.ksml.client.admin;

/*-
 * ========================LICENSE_START=================================
 * axual-client-proxy
 * %%
 * Copyright (C) 2020 Axual B.V.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ResolverUtil {
    private ResolverUtil() {
    }

    public static AclBindingFilter resolve(AclBindingFilter filter, TopicResolver topicResolver, GroupResolver groupResolver) {
        var name = filter.patternFilter().name();

        switch (filter.patternFilter().resourceType()) {
            case TOPIC -> name = topicResolver.resolveTopic(name);
            case GROUP -> name = groupResolver.resolveGroup(name);
        }

        return new AclBindingFilter(
                new ResourcePatternFilter(
                        filter.patternFilter().resourceType(),
                        name,
                        filter.patternFilter().patternType()),
                filter.entryFilter());
    }

    public static Collection<AclBinding> unresolveKeys(Collection<AclBinding> bindings, final TopicResolver topicResolver, final GroupResolver groupResolver) {
        Collection<AclBinding> result = new ArrayList<>(bindings.size());
        for (AclBinding binding : bindings) {
            String name = binding.pattern().name();

            switch (binding.pattern().resourceType()) {
                case TOPIC -> name = topicResolver.unresolveTopic(name);
                case GROUP -> name = groupResolver.unresolveGroup(name);
            }

            result.add(
                    new AclBinding(
                            new ResourcePattern(binding.pattern().resourceType(),
                                    name,
                                    binding.pattern().patternType()),
                            binding.entry()));
        }

        return result;
    }

    public static <T> Map<String, T> unresolveKeys(Map<String, T> map, TopicResolver resolver) {
        Map<String, T> result = new HashMap<>();
        map.forEach((key, value) -> result.put(resolver.unresolveTopic(key), value));
        return result;
    }
}
