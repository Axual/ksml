package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import static org.assertj.core.api.Assertions.assertThat;

class DataSchemaConstantsTest {

    @Test
    @DisplayName("isType recognises every known schema type name and rejects unknown ones")
    void isType() {
        assertThat(DataSchemaConstants.isType(DataSchemaConstants.STRING_TYPE)).isTrue();
        assertThat(DataSchemaConstants.isType(DataSchemaConstants.INTEGER_TYPE)).isTrue();
        assertThat(DataSchemaConstants.isType(DataSchemaConstants.UNION_TYPE)).isTrue();
        assertThat(DataSchemaConstants.isType(DataSchemaConstants.STRUCT_TYPE)).isTrue();
        assertThat(DataSchemaConstants.isType("not-a-type")).isFalse();
    }

    @Test
    @DisplayName("Constants carry their documented values")
    void constants() {
        final int noTag = DataSchemaConstants.NO_TAG;
        final String namespace = DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE;
        assertThat(noTag).isEqualTo(-1);
        assertThat(namespace).isEqualTo("io.axual.ksml.data");
    }
}
