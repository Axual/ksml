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


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CachedPatternResolver extends PatternResolver {
    private static final int DEFAULT_CACHE_SIZE = 128;
    private final LoadingCache<String, String> resolveCache;
    private final LoadingCache<String, Map<String, String>> unresolveCache;

    public CachedPatternResolver(String pattern, String defaultField, Map<String, String> defaultValues) {
        this(pattern, defaultField, defaultValues, DEFAULT_CACHE_SIZE);
    }

    public CachedPatternResolver(String pattern, String defaultField, Map<String, String> defaultValues, int cacheSize) {
        super(pattern, defaultField, defaultValues);
        // Specify the cache loading implementation. It contains the logic for topic unresolving
        resolveCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterAccess(Duration.ZERO)
                .expireAfterWrite(Duration.ZERO)
                .build(new ResolveCacheLoader());
        unresolveCache = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterAccess(Duration.ZERO)
                .expireAfterWrite(Duration.ZERO)
                .build(new UnresolveCacheLoader());
    }

    @Override
    public String resolve(String name) {
        try {
            var result = resolveCache.get(name);
            if (result != null) return result;
        } catch (ExecutionException e) {
            // Log and handle below as cache miss
            log.warn("Cache execution error while resolving \"{}\"", name, e);
        }

        return super.resolve(name);
    }

    @Override
    public String unresolve(String name) {
        try {
            var context = unresolveCache.get(name);
            if (context != null) return context.get(defaultFieldName);
        } catch (ExecutionException e) {
            // Log and handle below as cache miss
            log.warn("Cache execution error while unresolving \"{}\"", name, e);
        }

        return super.unresolve(name);
    }

    @Override
    public Map<String, String> unresolveContext(String name) {
        try {
            var context = unresolveCache.get(name);
            if (context != null) return context;
        } catch (ExecutionException e) {
            // Log and handle below as cache miss
            log.warn("Cache execution error while unresolving context of \"{}\"", name, e);
        }
        return super.unresolveContext(name);
    }

    private class ResolveCacheLoader extends CacheLoader<String, String> {
        @Override
        public String load(String name) throws Exception {
            return CachedPatternResolver.super.resolve(name);
        }
    }

    private class UnresolveCacheLoader extends CacheLoader<String, Map<String, String>> {
        @Override
        public Map<String, String> load(String name) {
            return CachedPatternResolver.super.unresolveContext(name);
        }
    }
}
