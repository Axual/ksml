package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.*;
import io.apicurio.registry.types.RoleType;
import io.apicurio.registry.types.RuleType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockRegistryClient implements RegistryClient {
    private long lastGlobalId = 0;

    private record RegistryEntry(long globalId, long contentId, String artifactId, byte[] schema) {
    }

    private final Map<String, RegistryEntry> entryByArtifactId = new HashMap<>();
    private final Map<Long, RegistryEntry> entryByGlobalId = new HashMap<>();
    private final Map<Long, List<ArtifactReference>> references = new HashMap<>();

    private RuntimeException error() {
        return new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getLatestArtifact(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public ArtifactMetaData updateArtifact(String groupId, String artifactId, String version, String artifactName, String artifactDescription, String contentType, InputStream data) {
        throw error();
    }

    @Override
    public ArtifactMetaData updateArtifact(String groupId, String artifactId, String version, String artifactName, String artifactDescription, InputStream data, List<ArtifactReference> references) {
        throw error();
    }

    @Override
    public void deleteArtifact(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public ArtifactMetaData getArtifactMetaData(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public ArtifactOwner getArtifactOwner(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public void updateArtifactMetaData(String groupId, String artifactId, EditableMetaData data) {
        throw error();
    }

    @Override
    public void updateArtifactOwner(String groupId, String artifactId, ArtifactOwner owner) {
        throw error();
    }

    @Override
    public VersionMetaData getArtifactVersionMetaDataByContent(String groupId, String artifactId, Boolean canonical, String contentType, InputStream data) {
        final var metadata = createArtifact(groupId, artifactId, contentType, IfExists.RETURN_OR_UPDATE, data);
        if (metadata == null) return null;
        return VersionMetaData.builder()
                .globalId(metadata.getGlobalId())
                .contentId(metadata.getContentId())
                .groupId(metadata.getGroupId())
                .build();
    }

    @Override
    public VersionMetaData getArtifactVersionMetaDataByContent(String groupId, String artifactId, Boolean canonical, ArtifactContent artifactContent) {
        throw error();
    }

    @Override
    public List<RuleType> listArtifactRules(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public void createArtifactRule(String groupId, String artifactId, Rule data) {
        throw error();
    }

    @Override
    public void deleteArtifactRules(String groupId, String artifactId) {
        throw error();
    }

    @Override
    public Rule getArtifactRuleConfig(String groupId, String artifactId, RuleType rule) {
        throw error();
    }

    @Override
    public Rule updateArtifactRuleConfig(String groupId, String artifactId, RuleType rule, Rule data) {
        throw error();
    }

    @Override
    public void deleteArtifactRule(String groupId, String artifactId, RuleType rule) {
        throw error();
    }

    @Override
    public List<Comment> getArtifactVersionComments(String groupId, String artifactId, String version) {
        throw error();
    }

    @Override
    public Comment addArtifactVersionComment(String groupId, String artifactId, String version, NewComment comment) {
        throw error();
    }

    @Override
    public void deleteArtifactVersionComment(String groupId, String artifactId, String version, String commentId) {
        throw error();
    }

    @Override
    public void editArtifactVersionComment(String groupId, String artifactId, String version, String commentId, NewComment comment) {
        throw error();
    }

    @Override
    public void updateArtifactState(String groupId, String artifactId, UpdateState data) {
        throw error();
    }

    @Override
    public void testUpdateArtifact(String groupId, String artifactId, String contentType, InputStream data) {
        throw error();
    }

    @Override
    public InputStream getArtifactVersion(String groupId, String artifactId, String version) {
        final var result = entryByGlobalId.get(Long.parseLong(artifactId));
        return result != null ? new ByteArrayInputStream(result.schema) : null;
    }

    @Override
    public VersionMetaData getArtifactVersionMetaData(String groupId, String artifactId, String version) {
        throw error();
    }

    @Override
    public void updateArtifactVersionMetaData(String groupId, String artifactId, String version, EditableMetaData data) {
        throw error();
    }

    @Override
    public void deleteArtifactVersionMetaData(String groupId, String artifactId, String version) {
        throw error();
    }

    @Override
    public void updateArtifactVersionState(String groupId, String artifactId, String version, UpdateState data) {
        throw error();
    }

    @Override
    public VersionSearchResults listArtifactVersions(String groupId, String artifactId, Integer offset, Integer limit) {
        throw error();
    }

    @Override
    public VersionMetaData createArtifactVersion(String groupId, String artifactId, String version, String artifactName, String artifactDescription, String contentType, InputStream data) {
        throw error();
    }

    @Override
    public ArtifactSearchResults listArtifactsInGroup(String groupId, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        throw error();
    }

    @Override
    public ArtifactMetaData createArtifact(String groupId, String artifactId, String version, String artifactType, IfExists ifExists, Boolean canonical, String artifactName, String artifactDescription, String contentType, String fromURL, String artifactSHA, InputStream data) {
        try {
            if (!entryByArtifactId.containsKey(artifactId)) {
                final var globalId = ++lastGlobalId;
                final var contentId = globalId + 10000;
                final var entry = new RegistryEntry(globalId, contentId, artifactId, data.readAllBytes());
                entryByArtifactId.put(artifactId, entry);
                entryByGlobalId.put(lastGlobalId, entry);
            }
            final var entry = entryByArtifactId.get(artifactId);
            return ArtifactMetaData.builder()
                    .globalId(entry.globalId)
                    .contentId(entry.contentId)
                    .id(artifactId)
                    .build();
        } catch (IOException e) {
            // Ignore
            return null;
        }
    }

    @Override
    public ArtifactMetaData createArtifact(String groupId, String artifactId, String version, String artifactType, IfExists ifExists, Boolean canonical, String artifactName, String artifactDescription, String contentType, String fromURL, String artifactSHA, InputStream data, List<ArtifactReference> artifactReferences) {
        throw error();
    }

    @Override
    public void deleteArtifactsInGroup(String groupId) {
        throw error();
    }

    @Override
    public void createArtifactGroup(GroupMetaData groupMetaData) {
        throw error();
    }

    @Override
    public void deleteArtifactGroup(String groupId) {
        throw error();
    }

    @Override
    public GroupMetaData getArtifactGroup(String groupId) {
        throw error();
    }

    @Override
    public GroupSearchResults listGroups(SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        throw error();
    }

    @Override
    public InputStream getContentById(long contentId) {
        throw error();
    }

    @Override
    public InputStream getContentByGlobalId(long globalId) {
        throw error();
    }

    @Override
    public InputStream getContentByGlobalId(long globalId, Boolean canonical, Boolean dereference) {
        final var schema = entryByGlobalId.get(globalId);
        return (schema != null) ? new ByteArrayInputStream(schema.schema) : null;
    }

    @Override
    public InputStream getContentByHash(String contentHash, Boolean canonical) {
        throw error();
    }

    @Override
    public ArtifactSearchResults searchArtifacts(String group, String name, String description, List<String> labels, List<String> properties, Long globalId, Long contentId, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        throw error();
    }

    @Override
    public ArtifactSearchResults searchArtifactsByContent(InputStream data, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        throw error();
    }

    @Override
    public List<RuleType> listGlobalRules() {
        throw error();
    }

    @Override
    public void createGlobalRule(Rule data) {
        throw error();
    }

    @Override
    public void deleteAllGlobalRules() {
        throw error();
    }

    @Override
    public Rule getGlobalRuleConfig(RuleType rule) {
        throw error();
    }

    @Override
    public Rule updateGlobalRuleConfig(RuleType rule, Rule data) {
        throw error();
    }

    @Override
    public void deleteGlobalRule(RuleType rule) {
        throw error();
    }

    @Override
    public List<NamedLogConfiguration> listLogConfigurations() {
        throw error();
    }

    @Override
    public NamedLogConfiguration getLogConfiguration(String logger) {
        throw error();
    }

    @Override
    public NamedLogConfiguration setLogConfiguration(String logger, LogConfiguration data) {
        throw error();
    }

    @Override
    public NamedLogConfiguration removeLogConfiguration(String logger) {
        throw error();
    }

    @Override
    public InputStream exportData() {
        throw error();
    }

    @Override
    public void importData(InputStream data) {
        throw error();
    }

    @Override
    public void importData(InputStream data, boolean preserveGlobalIds, boolean preserveContentIds) {
        throw error();
    }

    @Override
    public List<RoleMapping> listRoleMappings() {
        throw error();
    }

    @Override
    public void createRoleMapping(RoleMapping data) {
        throw error();
    }

    @Override
    public RoleMapping getRoleMapping(String principalId) {
        throw error();
    }

    @Override
    public void updateRoleMapping(String principalId, RoleType role) {
        throw error();
    }

    @Override
    public void deleteRoleMapping(String principalId) {
        throw error();
    }

    @Override
    public UserInfo getCurrentUserInfo() {
        throw error();
    }

    @Override
    public void setNextRequestHeaders(Map<String, String> requestHeaders) {
        throw error();
    }

    @Override
    public Map<String, String> getHeaders() {
        throw error();
    }

    @Override
    public List<ConfigurationProperty> listConfigProperties() {
        throw error();
    }

    @Override
    public void setConfigProperty(String propertyName, String propertyValue) {
        throw error();
    }

    @Override
    public ConfigurationProperty getConfigProperty(String propertyName) {
        throw error();
    }

    @Override
    public void deleteConfigProperty(String propertyName) {
        throw error();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByGlobalId(long globalId) {
        final var result = references.get(globalId);
        return result != null ? result : List.of();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentId(long contentId) {
        throw error();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentHash(String contentHash) {
        throw error();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByCoordinates(String groupId, String artifactId, String version) {
        return List.of();
    }

    @Override
    public void close() throws IOException {
        throw error();
    }

    public void setReferences(long globalId, List<ArtifactReference> refs) {
        references.put(globalId, refs);
    }
}
