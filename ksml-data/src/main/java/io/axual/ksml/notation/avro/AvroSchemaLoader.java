package io.axual.ksml.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import org.apache.avro.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaLibrary;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroSchemaLoader implements SchemaLibrary.Loader {
    private String schemaDirectory = "";

    public AvroSchemaLoader(String schemaDirectory) {
        this.schemaDirectory = schemaDirectory;
    }

    public DataSchema load(String schemaName) {
        // Load the schema with given (fully qualified) name
        while (true) {
            var result = loadInternal(schemaName);
            if (result != null) {
                return result;
            }
            if (!schemaName.contains(".")) break;
            schemaName = schemaName.substring(schemaName.indexOf(".") + 1);
        }

        throw new KSMLTopologyException("Could not load Avro schema: " + schemaName);
    }

    private DataSchema loadInternal(String schemaName) {
        var schemaFile = new File(schemaDirectory, schemaName + ".avsc");
        try (var schemaStream = new FileInputStream(schemaFile)) {
            var schema = new Schema.Parser().parse(schemaStream);
            return new AvroSchemaMapper().toDataSchema(schema);
        } catch (IOException e) {
            log.warn("Could not find/load Avro schema: " + schemaName);
            return null;
        }
    }
}
