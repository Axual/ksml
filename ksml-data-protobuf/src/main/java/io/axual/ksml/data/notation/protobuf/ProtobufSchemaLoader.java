package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML
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


import com.squareup.wire.schema.Location;
import io.apicurio.registry.rules.compatibility.protobuf.ProtobufCompatibilityCheckerLibrary;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.apicurio.registry.utils.protobuf.schema.ProtobufFile;
import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.schema.DataSchema;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class ProtobufSchemaLoader extends SchemaLoader {
    private static final ProtobufSchemaMapper MAPPER = new ProtobufSchemaMapper();

    public ProtobufSchemaLoader(String schemaDirectory) {
        super("PROTOBUF", schemaDirectory, ".proto");
    }

    @Override
    protected DataSchema parseSchema(String name, String schema) {
        runCompatibilityTest(name, schema);
        final var proto = new ProtobufSchemaParser<>().parseSchema(schema.getBytes(), Collections.emptyMap());
        return MAPPER.toDataSchema(name, proto);
    }

    private void runCompatibilityTest(String name, String schemaIn) {
        // This method checks for code completeness of ProtobufSchemaMapper. It will warn if schema translation to
        // DataSchema and back gives deltas between ProtobufSchemas.
        final var proto = new ProtobufSchemaParser<>().parseSchema(schemaIn.getBytes(), Collections.emptyMap());
        final var internalSchema = MAPPER.toDataSchema(name, proto);
        final var out = MAPPER.fromDataSchema(internalSchema);
        final var outFd = out.getFileDescriptor();
        final var outFe = out.getProtoFileElement();
        final var schemaOut = outFe.toSchema();
        final var checker = new ProtobufCompatibilityCheckerLibrary(new ProtobufFile(schemaIn), new ProtobufFile(schemaOut));
        final var diffs = checker.findDifferences();
        final var compatible = diffs.isEmpty();
        if (!compatible) {
            log.warn("PROTOBUF schema {} in/out is not compatible: {}", name, diffs);
        }
    }
}
