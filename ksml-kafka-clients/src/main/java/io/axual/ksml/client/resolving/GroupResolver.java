package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * axual-common
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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A group resolver can translate Kafka consumer group ids from an application's internal
 * representation to one found externally (or "physically") on Kafka clusters. The conversion from
 * internal to external representation is done through {@link #resolveGroup(String)}. The reverse is
 * done through {link #unresolveGroup(String)}.
 */
public interface GroupResolver extends Resolver {

    /**
     * Translates the internal representation of consumer group id to the external one.
     *
     * @param group the internal consumer group id
     * @return the external consumer group id
     */
    default String resolveGroup(final String group) {
        return resolve(group);
    }

    /**
     * Translates a collection of group ids using the internal format to a collection of group ids
     * using the external format
     *
     * @param groups the internal consumer group ids to resolve
     * @return A set of external consumer group ids
     */
    default Set<String> resolveGroups(final Collection<String> groups) {
        return groups == null ? Collections.emptySet() : groups.stream()
                .map(this::resolveGroup)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    /**
     * Translates the external representation of consumer group id to the internal one.
     *
     * @param group the external consumer group id
     * @return the internal consumer group id
     */
    default String unresolveGroup(final String group) {
        return unresolve(group);
    }

    /**
     * Translates a collection of group ids using the external format to a collection of group ids
     * using the internal format
     *
     * @param groups the external consumer group ids to resolve
     * @return A set of internal consumer group ids
     */
    default Set<String> unresolveGroups(final Collection<String> groups) {
        return groups == null ? Collections.emptySet() : groups.stream()
                .map(this::unresolveGroup)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
