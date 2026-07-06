package io.axual.ksml.client.admin;

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

import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.TopicResolver;

import java.util.Collection;
import java.util.Set;

/**
 * A simple test double that both resolves (by prefixing) and unresolves (by stripping the prefix)
 * topic and group names. Unknown, unprefixed names unresolve to {@code null} so the null-filtering
 * branches of the resolvers are exercised as well.
 */
final class PrefixResolver implements TopicResolver, GroupResolver {
    static final String PREFIX = "tenant-";

    @Override
    public String resolve(String name) {
        return name == null ? null : PREFIX + name;
    }

    @Override
    public String unresolve(String name) {
        if (name == null || !name.startsWith(PREFIX)) return null;
        return name.substring(PREFIX.length());
    }

    // TopicResolver and GroupResolver both declare these collection defaults, so disambiguate here.
    @Override
    public Set<String> resolve(Collection<String> names) {
        return TopicResolver.super.resolve(names);
    }

    @Override
    public Set<String> unresolve(Collection<String> names) {
        return TopicResolver.super.unresolve(names);
    }
}
