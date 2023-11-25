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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
public interface TransactionalIdResolver extends Resolver {

    /**
     * Translates the internal representation of producer transaction id to the external one.
     *
     * @param transactionalId the internal producer transaction id
     * @return the external producer transaction id
     */
    default String resolveTransactionalId(final String transactionalId) {
        return resolve(transactionalId);
    }

    /**
     * Translates a collection of txn ids using the internal format to a collection of txn ids
     * using the external format
     *
     * @param transactionalIds the internal producer txn ids to resolve
     * @return A set of external producer transaction ids
     */
    default Set<String> resolveTransactionalIds(final Collection<String> transactionalIds) {
        return transactionalIds == null ? Collections.emptySet() : transactionalIds.stream()
                .map(this::resolveTransactionalId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    /**
     * Translates the external representation of producer transaction id to the internal one.
     *
     * @param transactionalId the external producer transaction id
     * @return the internal producer transaction id
     */
    default String unresolveTransactionalId(final String transactionalId) {
        return unresolve(transactionalId);
    }

    /**
     * Translates a collection of transaction ids using the external format to a collection of transactionIds
     * using the internal format
     *
     * @param transactionalIds the external producer transaction ids to resolve
     * @return A set of internal producer transaction ids
     */
    default Set<String> unresolveTransactionalIds(final Collection<String> transactionalIds) {
        return transactionalIds == null ? Collections.emptySet() : transactionalIds.stream()
                .map(this::unresolveTransactionalId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
