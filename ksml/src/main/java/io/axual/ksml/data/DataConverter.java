package io.axual.ksml.data;

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

import io.axual.ksml.type.AvroType;
import io.axual.ksml.type.DataType;
import io.axual.ksml.type.StandardType;

public class DataConverter {
    public final DataType toType;
    private final AvroTypeConverter toAvro = new AvroTypeConverter();
    private final JsonTypeConverter toJson = new JsonTypeConverter();
    private final NativeTypeConverter toNative = new NativeTypeConverter();

    public DataConverter(DataType toType) {
        this.toType = toType;
    }

    public Object convert(Object object) {
        if (object == null) return null;

        if (toType instanceof AvroType) {
            return toAvro.convert(object);
        }

        if (toType == StandardType.JSON) {
            return toJson.convert(object);
        }

        return toNative.convert(object);
    }
}
