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

import java.util.Map;

/**
 * This class is able to resolve and unresolve patterns for Kafka transactional ids. The class implements
 * the {@link TransactionalIdResolver} interface.
 */
public class TransactionalIdPatternResolver extends CachedPatternResolver implements TransactionalIdResolver {
    private static final String DEFAULT_PLACEHOLDER_VALUE = "transactional.id";

    public TransactionalIdPatternResolver(String pattern, Map<String, String> defaultValues) {
        super(pattern, DEFAULT_PLACEHOLDER_VALUE, defaultValues);
    }
}
