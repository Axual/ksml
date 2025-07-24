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

import io.axual.ksml.data.notation.xml.XmlDataObjectMapper;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaMapper;
import org.junit.jupiter.api.Test;

class XmlTests {
    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest(XmlNotation.NOTATION_NAME, new XmlSchemaMapper());
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest(XmlNotation.NOTATION_NAME, new XmlDataObjectMapper(true));
    }

    @Test
    void serdeTest() {
        NotationTestRunner.serdeTest(new XmlNotation(new NotationContext(XmlNotation.NOTATION_NAME)), false);
    }
}
