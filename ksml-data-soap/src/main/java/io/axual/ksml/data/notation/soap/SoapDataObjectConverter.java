package io.axual.ksml.data.notation.soap;

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

import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;

public class SoapDataObjectConverter implements Notation.Converter {
    private static final SoapDataObjectMapper DATA_OBJECT_MAPPER = new SoapDataObjectMapper();
    private static final SoapStringMapper STRING_MAPPER = new SoapStringMapper();

    @Override
    public DataObject convert(DataObject value, DataType targetType) {
        // Convert from SOAP
        if (value.type() instanceof StructType) {
            // Convert from SOAP type to String
            if (targetType == DataString.DATATYPE) {
                return new DataString(STRING_MAPPER.toString(DATA_OBJECT_MAPPER.fromDataObject(value)));
            }
        }

        // Convert to SOAP
        if (targetType instanceof StructType) {
            // Convert from String to SOAP
            if (value instanceof DataString str) {
                return DATA_OBJECT_MAPPER.toDataObject(
                        new StructType(SoapSchema.generateSOAPSchema(DataSchema.ANY_SCHEMA)),
                        STRING_MAPPER.fromString(str.value()));
            }
        }

        // Return null if there is no conversion possible
        return null;
    }
}
