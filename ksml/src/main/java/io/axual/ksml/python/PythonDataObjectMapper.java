package io.axual.ksml.python;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.util.ExecutionUtil;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.Arrays;

public class PythonDataObjectMapper extends NativeDataObjectMapperWithSchema {
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();

    public PythonDataObjectMapper(boolean includeSchemaInfo) {
        super(includeSchemaInfo, includeSchemaInfo ? new PythonDataObjectMapper(false) : null);
    }

    @Override
    public DataObject toDataObject(DataType expected, Object object) {
        // If we got a polyglot Value object, then convert it below before letting the remainder be
        // handled by the superclass
        object = NATIVE_MAPPER.fromPython(object);

        // If we expect a union dataType, then check its value types, else convert by value.
        if (expected instanceof UnionType unionType)
            return polyglotValueToDataUnion(unionType, object);

        // Finally, convert all native types to DataObjects
        return super.toDataObject(expected, object);
    }

    private DataObject polyglotValueToDataUnion(UnionType unionType, Object value) {
        final var memberTypesInitialScan = new ArrayList<>(Arrays.asList(unionType.members()));
        // Remove any null memberTypes and scan the other types
        final var hasNullType = memberTypesInitialScan.removeIf(memberType -> DataNull.DATATYPE.equals(memberType.type()));
        for (final var memberType : memberTypesInitialScan) {
            final var result = ExecutionUtil.tryThis(() -> toDataObject(memberType.type(), value));
            if (result != null) return result;
        }
        // Handle the nullType scenario
        if (hasNullType) {
            var result = toDataObject(DataNull.DATATYPE, value);
            if (result != null) return result;
        }

        final var sourceValue = toDataObject(value);
        final var sourceValueStr = sourceValue != null ? sourceValue.toString(DataObject.Printer.EXTERNAL_NO_SCHEMA) : DataNull.INSTANCE.toString();
        final var sourceType = sourceValue != null ? sourceValue.type() : DataType.UNKNOWN;
        throw new DataException("Can not convert " + sourceType + " to " + unionType + ": value=" + sourceValueStr);
    }

    @Override
    public Value fromDataObject(DataObject object) {
        final var result = NATIVE_MAPPER.toPython(super.fromDataObject(object));
        return result instanceof Value value ? value : null;
    }
}
