package io.axual.ksml.notation.soap;

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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.notation.NotationConverter;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.dsl.SOAPSchema;
import io.axual.ksml.data.schema.AnySchema;

public class SOAPDataObjectConverter implements NotationConverter {
    private static final SOAPDataObjectMapper DATA_OBJECT_MAPPER = new SOAPDataObjectMapper();
    private static final SOAPStringMapper STRING_MAPPER = new SOAPStringMapper();

    @Override
    public DataObject convert(DataObject value, UserType targetType) {
        // Convert from SOAP
        if (value.type() instanceof StructType) {
            // Convert from SOAP type to String
            if (targetType.dataType() == DataString.DATATYPE) {
                return new DataString(STRING_MAPPER.toString(DATA_OBJECT_MAPPER.fromDataObject(value)));
            }
        }

        // Convert to SOAP
        if (targetType.dataType() instanceof StructType) {
            // Convert from String to SOAP
            if (value instanceof DataString str) {
                return DATA_OBJECT_MAPPER.toDataObject(
                        new StructType(SOAPSchema.generateSOAPSchema(AnySchema.INSTANCE)),
                        STRING_MAPPER.fromString(str.value()));
            }
        }

        // Return null if there is no conversion possible
        return null;
    }
}
