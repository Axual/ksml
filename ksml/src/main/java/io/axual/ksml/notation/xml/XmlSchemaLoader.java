package io.axual.ksml.notation.xml;

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

import io.axual.ksml.execution.FatalError;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class XmlSchemaLoader implements SchemaLibrary.Loader {
    private String schemaDirectory = "";

    public XmlSchemaLoader(String schemaDirectory) {
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

        throw FatalError.dataError("Could not load Xml schema: " + schemaName);
    }

    private DataSchema loadInternal(String schemaName) {
        return null;
//        var schemaFile = new File(schemaDirectory, schemaName + ".xsd");
//        try (var schemaStream = new FileInputStream(schemaFile)) {
//            var schema = new javax.xml.validation.Schema;
//            return new XmlSchemaMapper().toDataSchema(schema);
//
//
//
//            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
//            Source schemaFile = new StreamSource(getFile(xsdPath));
//            Schema schema = factory.newSchema(schemaFile);
//            return schema.newValidator();
//
//
//
//        } catch (IOException e) {
//            log.warn("Could not find/load Xml schema: " + schemaName);
//            return null;
//        }
    }
}
