package io.axual.ksml.data.notation;

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

import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.notation.xml.XmlDataObjectMapper;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaMapper;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_DATA_FIELD_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_ENUM_SCHEMA_NAMESPACE;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_ENUM_SCHEMA_SYMBOL_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_ENUM_SCHEMA_SYMBOL_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_UNION_SCHEMA_MEMBER_NAME;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_UNION_SCHEMA_MEMBER_TAG;

class XmlTests {
    private static final EqualityFlags XML_EQUALITY_FLAGS = new EqualityFlags(
            IGNORE_DATA_FIELD_TAG,
            IGNORE_ENUM_SCHEMA_NAMESPACE,
            IGNORE_ENUM_SCHEMA_SYMBOL_DOC,
            IGNORE_ENUM_SCHEMA_SYMBOL_TAG,
            IGNORE_UNION_SCHEMA_MEMBER_NAME,
            IGNORE_UNION_SCHEMA_MEMBER_TAG
    );

    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest(XmlNotation.NOTATION_NAME, new XmlSchemaMapper(), XML_EQUALITY_FLAGS);
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest(XmlNotation.NOTATION_NAME, new XmlDataObjectMapper(true), XML_EQUALITY_FLAGS);
    }

    @Test
    void serdeTest() {
        NotationTestRunner.serdeTest(new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME)), false, XML_EQUALITY_FLAGS);
    }
}
