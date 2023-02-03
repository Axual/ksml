package io.axual.ksml.data.object;

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

import io.axual.ksml.data.type.SimpleType;

public class DataBytes extends DataPrimitive<byte[]> {
    public static final SimpleType DATATYPE = new SimpleType(byte[].class) {
        @Override
        public String containerName() {
            return "Bytes";
        }
    };
    public static final byte[] DEFAULT = new byte[0];

    public DataBytes() {
        this(DEFAULT);
    }

    public DataBytes(byte[] value) {
        super(DATATYPE, value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(type().toString()).append(": [");
        for (int index = 0; index < value().length; index++) {
            if (index > 0) builder.append(", ");
            builder.append(String.format("%02X ", value()[index]));
        }
        builder.append("]");
        return builder.toString();
    }
}
