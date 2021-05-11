package io.axual.ksml.parser;

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

import io.axual.ksml.exception.KSMLParseException;

public class SchemaLoader {
    private static String schemaDirectory = "";

    private SchemaLoader() {
    }

    public static void setSchemaDirectory(String directory) {
        schemaDirectory = directory;
    }

    public static Schema load(String schemaName) {
        File schemaFile = new File(schemaDirectory, schemaName + ".avsc");
        try (FileInputStream schemaStream = new FileInputStream(schemaFile)) {
            return new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new KSMLParseException("Could not parse Avro Schema from file: " + schemaFile.getAbsolutePath());
        }
    }
}
