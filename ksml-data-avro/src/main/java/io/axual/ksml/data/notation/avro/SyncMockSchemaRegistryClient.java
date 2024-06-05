package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class SyncMockSchemaRegistryClient implements SchemaRegistryClient {
    @Override
    public synchronized Optional<ParsedSchema> parseSchema(String schemaType, String schemaString, List<SchemaReference> references) {
        return wrappedClient.parseSchema(schemaType, schemaString, references);
    }

    @Override
    public synchronized Optional<ParsedSchema> parseSchema(Schema schema) {
        return wrappedClient.parseSchema(schema);
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
        return wrappedClient.register(subject, schema);
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, boolean normalize) throws IOException, RestClientException {
        return wrappedClient.register(subject, schema, normalize);
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, int version, int id) throws IOException, RestClientException {
        return wrappedClient.register(subject, schema, version, id);
    }

    @Override
    public synchronized RegisterSchemaResponse registerWithResponse(String subject, ParsedSchema schema, boolean normalize) throws RestClientException {
        return wrappedClient.registerWithResponse(subject, schema, normalize);
    }

    @Override
    public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
        return wrappedClient.getSchemaById(id);
    }

    @Override
    public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        return wrappedClient.getSchemaBySubjectAndId(subject, id);
    }

    @Override
    public synchronized List<ParsedSchema> getSchemas(String subjectPrefix, boolean lookupDeletedSchema, boolean latestOnly) throws IOException, RestClientException {
        return wrappedClient.getSchemas(subjectPrefix, lookupDeletedSchema, latestOnly);
    }

    @Override
    public synchronized Collection<String> getAllSubjectsById(int id) {
        return wrappedClient.getAllSubjectsById(id);
    }

    @Override
    public synchronized Collection<SubjectVersion> getAllVersionsById(int id) {
        return wrappedClient.getAllVersionsById(id);
    }

    @Override
    public synchronized Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
        return wrappedClient.getByVersion(subject, version, lookupDeletedSchema);
    }

    @Override
    public synchronized SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
        return wrappedClient.getSchemaMetadata(subject, version);
    }

    @Override
    public synchronized SchemaMetadata getSchemaMetadata(String subject, int version, boolean lookupDeletedSchema) throws RestClientException {
        return wrappedClient.getSchemaMetadata(subject, version, lookupDeletedSchema);
    }

    @Override
    public synchronized SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
        return wrappedClient.getLatestSchemaMetadata(subject);
    }

    @Override
    public synchronized SchemaMetadata getLatestWithMetadata(String subject, Map<String, String> metadata, boolean lookupDeletedSchema) throws IOException, RestClientException {
        return wrappedClient.getLatestWithMetadata(subject, metadata, lookupDeletedSchema);
    }

    @Override
    public synchronized int getVersion(String subject, ParsedSchema schema) throws IOException, RestClientException {
        return wrappedClient.getVersion(subject, schema);
    }

    @Override
    public synchronized int getVersion(String subject, ParsedSchema schema, boolean normalize) throws IOException, RestClientException {
        return wrappedClient.getVersion(subject, schema, normalize);
    }

    @Override
    public synchronized List<Integer> getAllVersions(String subject) throws IOException, RestClientException {
        return wrappedClient.getAllVersions(subject);
    }

    @Override
    public synchronized boolean testCompatibility(String subject, ParsedSchema newSchema) throws IOException, RestClientException {
        return wrappedClient.testCompatibility(subject, newSchema);
    }

    @Override
    public synchronized int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
        return wrappedClient.getId(subject, schema);
    }

    @Override
    public synchronized int getId(String subject, ParsedSchema schema, boolean normalize) throws IOException, RestClientException {
        return wrappedClient.getId(subject, schema, normalize);
    }

    @Override
    public synchronized List<Integer> deleteSubject(String subject, boolean isPermanent) throws IOException, RestClientException {
        return wrappedClient.deleteSubject(subject, isPermanent);
    }

    @Override
    public synchronized List<Integer> deleteSubject(Map<String, String> requestProperties, String subject, boolean isPermanent) throws IOException, RestClientException {
        return wrappedClient.deleteSubject(requestProperties, subject, isPermanent);
    }

    @Override
    public synchronized Integer deleteSchemaVersion(String subject, String version, boolean isPermanent) throws IOException, RestClientException {
        return wrappedClient.deleteSchemaVersion(subject, version, isPermanent);
    }

    @Override
    public synchronized Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version, boolean isPermanent) throws IOException, RestClientException {
        return wrappedClient.deleteSchemaVersion(requestProperties, subject, version, isPermanent);
    }

    @Override
    public synchronized List<String> testCompatibilityVerbose(String subject, ParsedSchema newSchema) throws IOException, RestClientException {
        return wrappedClient.testCompatibilityVerbose(subject, newSchema);
    }

    @Override
    public synchronized Config updateConfig(String subject, Config config) throws IOException, RestClientException {
        return wrappedClient.updateConfig(subject, config);
    }

    @Override
    public synchronized Config getConfig(String subject) throws IOException, RestClientException {
        return wrappedClient.getConfig(subject);
    }

    @Override
    public synchronized String setMode(String mode) throws IOException, RestClientException {
        return wrappedClient.setMode(mode);
    }

    @Override
    public synchronized String setMode(String mode, String subject) throws IOException, RestClientException {
        return wrappedClient.setMode(mode, subject);
    }

    @Override
    public synchronized String setMode(String mode, String subject, boolean force) throws IOException, RestClientException {
        return wrappedClient.setMode(mode, subject, force);
    }

    @Override
    public synchronized String getMode() throws IOException, RestClientException {
        return wrappedClient.getMode();
    }

    @Override
    public synchronized String getMode(String subject) throws IOException, RestClientException {
        return wrappedClient.getMode(subject);
    }

    @Override
    public synchronized Collection<String> getAllSubjects() throws IOException, RestClientException {
        return wrappedClient.getAllSubjects();
    }

    @Override
    public synchronized Collection<String> getAllSubjectsByPrefix(String subjectPrefix) throws IOException, RestClientException {
        return wrappedClient.getAllSubjectsByPrefix(subjectPrefix);
    }

    @Override
    public synchronized void reset() {
        wrappedClient.reset();
    }

    private final MockSchemaRegistryClient wrappedClient = new MockSchemaRegistryClient();
}
