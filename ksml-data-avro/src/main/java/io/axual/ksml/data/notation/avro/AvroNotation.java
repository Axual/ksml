package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

public class AvroNotation extends BaseNotation {
    public static final String NOTATION_NAME = "avro";
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final AvroSchemaParser AVRO_SCHEMA_PARSER = new AvroSchemaParser();
    @Getter
    private final AvroSerdeSupplier serdeSupplier;

    public AvroNotation(AvroSerdeSupplier serdeSupplier) {
        super(NOTATION_NAME, serdeSupplier.vendorName(), ".avsc", DEFAULT_TYPE, null, AVRO_SCHEMA_PARSER);
        this.serdeSupplier = serdeSupplier;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        return serdeSupplier.get(type, isKey);
    }
}
