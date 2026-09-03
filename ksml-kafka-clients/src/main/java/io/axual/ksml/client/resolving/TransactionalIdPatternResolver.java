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

import java.util.Map;
public class TransactionalIdPatternResolver implements TransactionalIdResolver {
    public static final String DEFAULT_PLACEHOLDER_VALUE = "transactional.id";
    public static final String DEFAULT_PLACEHOLDER_PATTERN = PatternResolver.FIELD_NAME_PREFIX + DEFAULT_PLACEHOLDER_VALUE + PatternResolver.FIELD_NAME_SUFFIX;
    private final CachedPatternResolver delegate;

    public TransactionalIdPatternResolver(String pattern, Map<String, String> defaultValues) {
        delegate = new CachedPatternResolver(pattern, DEFAULT_PLACEHOLDER_VALUE, defaultValues);
    }

    @Override
    public String resolve(String name) {
        return delegate.resolve(name);
    }

    @Override
    public String unresolve(String name) {
        return delegate.unresolve(name);
    }

    public Map<String, String> unresolveContext(String name) {
        return delegate.unresolveContext(name);
    }
}
