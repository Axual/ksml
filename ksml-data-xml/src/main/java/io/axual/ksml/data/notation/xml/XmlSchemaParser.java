package io.axual.ksml.data.notation.xml;

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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.schema.DataSchema;

public class XmlSchemaParser implements Notation.SchemaParser {
    private final XmlSchemaMapper schemaMapper;

    public XmlSchemaParser(NativeDataObjectMapper nativeMapper, DataTypeDataSchemaMapper typeMapper) {
        schemaMapper = new XmlSchemaMapper(nativeMapper, typeMapper);
    }

    @Override
    public DataSchema parse(String contextName, String schemaName, String schemaString) {
        return schemaMapper.toDataSchema(null, schemaName, schemaString);
    }
}