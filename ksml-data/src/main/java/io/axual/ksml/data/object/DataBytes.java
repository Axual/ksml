package io.axual.ksml.data.object;

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

import io.axual.ksml.data.parser.schema.DataSchemaDSL;
import io.axual.ksml.data.type.SimpleType;

import java.util.Arrays;

public class DataBytes extends DataPrimitive<byte[]> {
    public static final SimpleType DATATYPE = new SimpleType(byte[].class, DataSchemaDSL.BYTES_TYPE);

    public DataBytes() {
        this(null);
    }

    public DataBytes(byte[] value) {
        super(DATATYPE, value != null ? Arrays.copyOf(value, value.length) : null);
    }

    @Override
    public String toString(Printer printer) {
        final var sb = new StringBuilder(printer.schemaString(this));
        if (value() == null) return sb.append("null").toString();
        sb.append("[");
        for (int index = 0; index < value().length; index++) {
            if (index > 0) sb.append(", ");
            sb.append(String.format("%02X ", value()[index]));
        }
        sb.append("]");
        return sb.toString();
    }
}
