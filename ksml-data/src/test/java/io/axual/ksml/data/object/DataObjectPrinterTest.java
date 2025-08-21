package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class DataObjectPrinterTest {

    @Test
    @DisplayName("childObjectPrinter transitions are as specified")
    void childObjectPrinterTransitions() {
        assertThat(INTERNAL.childObjectPrinter()).isEqualTo(EXTERNAL_NO_SCHEMA);
        assertThat(EXTERNAL_NO_SCHEMA.childObjectPrinter()).isEqualTo(EXTERNAL_NO_SCHEMA);
        assertThat(EXTERNAL_TOP_SCHEMA.childObjectPrinter()).isEqualTo(EXTERNAL_NO_SCHEMA);
        assertThat(EXTERNAL_ALL_SCHEMA.childObjectPrinter()).isEqualTo(EXTERNAL_ALL_SCHEMA);
    }

    @Test
    @DisplayName("forceSchemaString always returns '<type>: '")
    void forceSchemaStringAlwaysShowsType() {
        var ds = new DataString("x");
        assertThat(INTERNAL.forceSchemaString(ds)).isEqualTo("string: ");
        assertThat(EXTERNAL_NO_SCHEMA.forceSchemaString(ds)).isEqualTo("string: ");
        assertThat(EXTERNAL_TOP_SCHEMA.forceSchemaString(ds)).isEqualTo("string: ");
        assertThat(EXTERNAL_ALL_SCHEMA.forceSchemaString(ds)).isEqualTo("string: ");
    }

    @Test
    @DisplayName("schemaString honors printer mode: INTERNAL/EXTERNAL_NO_SCHEMA empty; TOP/ALL show type")
    void schemaStringFollowsMode() {
        var ds = new DataString("y");
        assertThat(INTERNAL.schemaString(ds)).isEmpty();
        assertThat(EXTERNAL_NO_SCHEMA.schemaString(ds)).isEmpty();
        assertThat(EXTERNAL_TOP_SCHEMA.schemaString(ds)).isEqualTo("string: ");
        assertThat(EXTERNAL_ALL_SCHEMA.schemaString(ds)).isEqualTo("string: ");
    }
}
