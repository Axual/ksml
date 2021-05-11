package io.axual.ksml.type;

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



import io.axual.ksml.exception.KSMLTopologyException;

public class KeyValueType extends TupleType {
    public KeyValueType(DataType keyType, DataType valueType) {
        super(new DataType[]{keyType, valueType});
    }

    public DataType getKeyType() {
        return subTypes[0];
    }

    public DataType getValueType() {
        return subTypes[1];
    }

    public static KeyValueType createFrom(DataType type) {
        if (type instanceof TupleType && ((TupleType) type).subTypes.length == 2) {
            return new KeyValueType(((TupleType) type).subTypes[0], ((TupleType) type).subTypes[1]);
        }
        throw new KSMLTopologyException("Could not convert type to KeyValue: " + type);
    }
}
