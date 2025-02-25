package io.axual.ksml.notation.protobuf;

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
    private long counter = 0;
    private final Map<Long, byte[]> content = new HashMap<>();

    @Override
    public InputStream getLatestArtifact(String groupId, String artifactId) {
        return null;
    }

    @Override
    public ArtifactMetaData updateArtifact(String groupId, String artifactId, String version, String artifactName, String artifactDescription, String contentType, InputStream data) {
        return null;
    }

    @Override
    public ArtifactMetaData updateArtifact(String groupId, String artifactId, String version, String artifactName, String artifactDescription, InputStream data, List<ArtifactReference> references) {
        return null;
    }

    @Override
    public void deleteArtifact(String groupId, String artifactId) {

    }

    @Override
    public ArtifactMetaData getArtifactMetaData(String groupId, String artifactId) {
        return null;
    }

    @Override
    public ArtifactOwner getArtifactOwner(String groupId, String artifactId) {
        return null;
    }

    @Override
    public void updateArtifactMetaData(String groupId, String artifactId, EditableMetaData data) {

    }

    @Override
    public void updateArtifactOwner(String groupId, String artifactId, ArtifactOwner owner) {

    }

    @Override
    public VersionMetaData getArtifactVersionMetaDataByContent(String groupId, String artifactId, Boolean canonical, String contentType, InputStream data) {
        try {
            content.put(++counter, data.readAllBytes());
            return VersionMetaData.builder()
                    .globalId(counter)
                    .contentId(counter)
                    .build();
        } catch (IOException e) {
            // Ignore
            return null;
        }
    }

    @Override
    public VersionMetaData getArtifactVersionMetaDataByContent(String groupId, String artifactId, Boolean canonical, ArtifactContent artifactContent) {
        return null;
    }

    @Override
    public List<RuleType> listArtifactRules(String groupId, String artifactId) {
        return List.of();
    }

    @Override
    public void createArtifactRule(String groupId, String artifactId, Rule data) {

    }

    @Override
    public void deleteArtifactRules(String groupId, String artifactId) {

    }

    @Override
    public Rule getArtifactRuleConfig(String groupId, String artifactId, RuleType rule) {
        return null;
    }

    @Override
    public Rule updateArtifactRuleConfig(String groupId, String artifactId, RuleType rule, Rule data) {
        return null;
    }

    @Override
    public void deleteArtifactRule(String groupId, String artifactId, RuleType rule) {

    }

    @Override
    public List<Comment> getArtifactVersionComments(String groupId, String artifactId, String version) {
        return List.of();
    }

    @Override
    public Comment addArtifactVersionComment(String groupId, String artifactId, String version, NewComment comment) {
        return null;
    }

    @Override
    public void deleteArtifactVersionComment(String groupId, String artifactId, String version, String commentId) {

    }

    @Override
    public void editArtifactVersionComment(String groupId, String artifactId, String version, String commentId, NewComment comment) {

    }

    @Override
    public void updateArtifactState(String groupId, String artifactId, UpdateState data) {

    }

    @Override
    public void testUpdateArtifact(String groupId, String artifactId, String contentType, InputStream data) {

    }

    @Override
    public InputStream getArtifactVersion(String groupId, String artifactId, String version) {
        return null;
    }

    @Override
    public VersionMetaData getArtifactVersionMetaData(String groupId, String artifactId, String version) {
        return null;
    }

    @Override
    public void updateArtifactVersionMetaData(String groupId, String artifactId, String version, EditableMetaData data) {

    }

    @Override
    public void deleteArtifactVersionMetaData(String groupId, String artifactId, String version) {

    }

    @Override
    public void updateArtifactVersionState(String groupId, String artifactId, String version, UpdateState data) {

    }

    @Override
    public VersionSearchResults listArtifactVersions(String groupId, String artifactId, Integer offset, Integer limit) {
        return null;
    }

    @Override
    public VersionMetaData createArtifactVersion(String groupId, String artifactId, String version, String artifactName, String artifactDescription, String contentType, InputStream data) {
        return null;
    }

    @Override
    public ArtifactSearchResults listArtifactsInGroup(String groupId, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        return null;
    }

    @Override
    public ArtifactMetaData createArtifact(String groupId, String artifactId, String version, String artifactType, IfExists ifExists, Boolean canonical, String artifactName, String artifactDescription, String contentType, String fromURL, String artifactSHA, InputStream data) {
        return null;
    }

    @Override
    public ArtifactMetaData createArtifact(String groupId, String artifactId, String version, String artifactType, IfExists ifExists, Boolean canonical, String artifactName, String artifactDescription, String contentType, String fromURL, String artifactSHA, InputStream data, List<ArtifactReference> artifactReferences) {
        return null;
    }

    @Override
    public void deleteArtifactsInGroup(String groupId) {

    }

    @Override
    public void createArtifactGroup(GroupMetaData groupMetaData) {

    }

    @Override
    public void deleteArtifactGroup(String groupId) {

    }

    @Override
    public GroupMetaData getArtifactGroup(String groupId) {
        return null;
    }

    @Override
    public GroupSearchResults listGroups(SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        return null;
    }

    @Override
    public InputStream getContentById(long contentId) {
        return null;
    }

    @Override
    public InputStream getContentByGlobalId(long globalId) {
        return null;
    }

    @Override
    public InputStream getContentByGlobalId(long globalId, Boolean canonical, Boolean dereference) {
        final var schema = content.get(globalId);
        if (schema != null) return new ByteArrayInputStream(schema);
        return null;
    }

    @Override
    public InputStream getContentByHash(String contentHash, Boolean canonical) {
        return null;
    }

    @Override
    public ArtifactSearchResults searchArtifacts(String group, String name, String description, List<String> labels, List<String> properties, Long globalId, Long contentId, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        return null;
    }

    @Override
    public ArtifactSearchResults searchArtifactsByContent(InputStream data, SortBy orderBy, SortOrder order, Integer offset, Integer limit) {
        return null;
    }

    @Override
    public List<RuleType> listGlobalRules() {
        return List.of();
    }

    @Override
    public void createGlobalRule(Rule data) {

    }

    @Override
    public void deleteAllGlobalRules() {

    }

    @Override
    public Rule getGlobalRuleConfig(RuleType rule) {
        return null;
    }

    @Override
    public Rule updateGlobalRuleConfig(RuleType rule, Rule data) {
        return null;
    }

    @Override
    public void deleteGlobalRule(RuleType rule) {

    }

    @Override
    public List<NamedLogConfiguration> listLogConfigurations() {
        return List.of();
    }

    @Override
    public NamedLogConfiguration getLogConfiguration(String logger) {
        return null;
    }

    @Override
    public NamedLogConfiguration setLogConfiguration(String logger, LogConfiguration data) {
        return null;
    }

    @Override
    public NamedLogConfiguration removeLogConfiguration(String logger) {
        return null;
    }

    @Override
    public InputStream exportData() {
        return null;
    }

    @Override
    public void importData(InputStream data) {

    }

    @Override
    public void importData(InputStream data, boolean preserveGlobalIds, boolean preserveContentIds) {

    }

    @Override
    public List<RoleMapping> listRoleMappings() {
        return List.of();
    }

    @Override
    public void createRoleMapping(RoleMapping data) {

    }

    @Override
    public RoleMapping getRoleMapping(String principalId) {
        return null;
    }

    @Override
    public void updateRoleMapping(String principalId, RoleType role) {

    }

    @Override
    public void deleteRoleMapping(String principalId) {

    }

    @Override
    public UserInfo getCurrentUserInfo() {
        return null;
    }

    @Override
    public void setNextRequestHeaders(Map<String, String> requestHeaders) {

    }

    @Override
    public Map<String, String> getHeaders() {
        return Map.of();
    }

    @Override
    public List<ConfigurationProperty> listConfigProperties() {
        return List.of();
    }

    @Override
    public void setConfigProperty(String propertyName, String propertyValue) {

    }

    @Override
    public ConfigurationProperty getConfigProperty(String propertyName) {
        return null;
    }

    @Override
    public void deleteConfigProperty(String propertyName) {

    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByGlobalId(long globalId) {
        return List.of();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentId(long contentId) {
        return List.of();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByContentHash(String contentHash) {
        return List.of();
    }

    @Override
    public List<ArtifactReference> getArtifactReferencesByCoordinates(String groupId, String artifactId, String version) {
        return List.of();
    }

    @Override
    public void close() throws IOException {

    }
}
