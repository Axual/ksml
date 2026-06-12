package io.axual.ksml.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.apicurio.registry.resolver.client.RegistryArtifactReference;
import io.apicurio.registry.resolver.client.RegistryClientFacade;
import io.apicurio.registry.resolver.client.RegistryVersionCoordinates;
import io.apicurio.registry.resolver.config.SchemaResolverConfig;
import io.apicurio.registry.resolver.strategy.ArtifactReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MockApicurioSchemaRegistryClient implements RegistryClientFacade {
    private long lastGlobalId = 0;
    private long lastContentId = 10000;

    private record RegistryEntry(long globalId, long contentId, String artifactId, String schema) {
    }

    private final Map<String, RegistryEntry> entryByArtifactId = new HashMap<>();
    private final Map<Long, RegistryEntry> entryByGlobalId = new HashMap<>();
    private final Map<Long, RegistryEntry> entryByContentId = new HashMap<>();
    private final Map<Long, List<RegistryArtifactReference>> references = new HashMap<>();

    public Map<String, String> configs() {
        final var result = new HashMap<String, String>();
        result.putIfAbsent(SchemaResolverConfig.AUTO_REGISTER_ARTIFACT, "true");
        return result;
    }

    private RuntimeException error() {
        return new RuntimeException("Not implemented");
    }

    private RegistryVersionCoordinates registerOrFind(String artifactId, String schemaString) {
        if (!entryByArtifactId.containsKey(artifactId)) {
            final var globalId = ++lastGlobalId;
            final var contentId = ++lastContentId;
            final var entry = new RegistryEntry(globalId, contentId, artifactId, schemaString);
            entryByArtifactId.put(artifactId, entry);
            entryByGlobalId.put(globalId, entry);
            entryByContentId.put(contentId, entry);
        }
        final var entry = entryByArtifactId.get(artifactId);
        return RegistryVersionCoordinates.create(entry.globalId, entry.contentId, null, entry.artifactId, null);
    }

    @Override
    public String getSchemaByContentId(Long contentId) {
        final var entry = entryByContentId.get(contentId);
        return entry != null ? entry.schema : null;
    }

    @Override
    public String getSchemaByGlobalId(long globalId, boolean dereferenced) {
        final var entry = entryByGlobalId.get(globalId);
        return entry != null ? entry.schema : null;
    }

    @Override
    public String getSchemaByGAV(String groupId, String artifactId, String version) {
        final var entry = entryByArtifactId.get(artifactId);
        return entry != null ? entry.schema : null;
    }

    @Override
    public String getSchemaByContentHash(String contentHash) {
        throw error();
    }

    @Override
    public List<RegistryArtifactReference> getReferencesByContentId(long contentId) {
        final var result = references.get(contentId);
        return result != null ? result : List.of();
    }

    @Override
    public List<RegistryArtifactReference> getReferencesByGlobalId(long globalId) {
        final var result = references.get(globalId);
        return result != null ? result : List.of();
    }

    @Override
    public List<RegistryArtifactReference> getReferencesByGAV(String groupId, String artifactId, String version) {
        return List.of();
    }

    @Override
    public List<RegistryArtifactReference> getReferencesByContentHash(String contentHash) {
        return List.of();
    }

    @Override
    public List<RegistryVersionCoordinates> searchVersionsByContent(String schemaString, String artifactType, ArtifactReference reference, boolean canonical) {
        final var artifactId = reference != null ? reference.getArtifactId() : null;
        if (artifactId == null) return List.of();
        final var entry = entryByArtifactId.get(artifactId);
        if (entry == null) return List.of();
        return List.of(RegistryVersionCoordinates.create(entry.globalId, entry.contentId, null, entry.artifactId, null));
    }

    @Override
    public RegistryVersionCoordinates createSchema(String artifactType, String groupId, String artifactId, String version, String autoCreateBehavior, boolean canonical, String schemaString, Set<RegistryArtifactReference> schemaReferences) {
        return registerOrFind(artifactId, schemaString);
    }

    @Override
    public RegistryVersionCoordinates getVersionCoordinatesByGAV(String groupId, String artifactId, String version) {
        final var entry = entryByArtifactId.get(artifactId);
        if (entry == null) return null;
        return RegistryVersionCoordinates.create(entry.globalId, entry.contentId, null, entry.artifactId, null);
    }

    @Override
    public Object getClient() {
        return null;
    }

    public void setReferences(long globalId, List<RegistryArtifactReference> refs) {
        references.put(globalId, refs);
    }
}