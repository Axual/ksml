package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.schema.LogicalSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.schema.logical.LogicalTypeConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A logical type is an Avro concept. PROTOBUF has no equivalent, so the mapper falls back to the base
 * primitive instead of failing. Before this was handled, a struct read from Avro and written to PROTOBUF
 * failed with "Can not convert schema type bytes to PROTOBUF type", which is misleading because bytes is
 * supported.
 */
class ProtobufLogicalSchemaTest {
    private final ProtobufFileElementSchemaMapper mapper =
            new ProtobufFileElementSchemaMapper(new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());

    private static StructSchema structWith(LogicalSchema logical) {
        return new StructSchema("io.axual.test", "WithLogical", "test",
                List.of(new StructSchema.Field("field", logical, "a logical field")));
    }

    @Test
    @DisplayName("A decimal field converts as its base bytes type")
    void decimalConvertsAsBytes() {
        assertThatCode(() -> mapper.fromDataSchema(structWith(new LogicalSchema(new DecimalLogicalType(10, 2)))))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("A uuid field converts as its base string type")
    void uuidConvertsAsString() {
        assertThatCode(() -> mapper.fromDataSchema(structWith(new LogicalSchema(LogicalTypeConstants.UUID_TYPE))))
                .doesNotThrowAnyException();
    }
}
