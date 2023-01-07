package io.axual.ksml.util;

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

import org.apache.kafka.streams.kstream.Windowed;

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.schema.SchemaUtil;
import io.axual.ksml.schema.mapper.WindowedSchemaMapper;

import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.END_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.END_TIME_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.KEY_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.START_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.START_TIME_FIELD;

public class DataUtil {
    private static final NativeDataObjectMapper nativeDataObjectMapper = new NativeDataObjectMapper();
    private static final WindowedSchemaMapper windowedSchemaMapper = new WindowedSchemaMapper();

    private DataUtil() {
    }

    // KSML uses a generic policy that ALL data in streams is internally represented as DataObjects.
    // However, Kafka Streams also dictates some types of its own, namely classes like Windowed and
    // Long, which are both used in the count() operation. This leads to ClassCastExceptions if we
    // define our stream types as KStream<DataObject,DataObject>, since Windows and Long cannot be
    // cast to DataObject. Therefore, all formal stream types in the Java code are
    // KStream<Object,Object> instead of KStream<DataObject,DataObject>. This method ensures that
    // any data dataType injected by Kafka Streams gets modified into a proper DataObject on the
    // fly. When new data types pop up in Kafka Streams' generics, add the conversion to a
    // DataObject to this method.
    public static DataObject asDataObject(Object object) {
        if (object instanceof DataObject dataObject) return dataObject;
        if (object instanceof Windowed<?> windowedObject) {
            // Convert a Windowed object into a struct with fields that contain the window fields.
            var keyAsData = asDataObject(windowedObject.key());
            var schema = windowedSchemaMapper.toDataSchema(new WindowedType(keyAsData.type()));
            var result = new DataStruct(new StructType(schema));
            result.put(START_FIELD, new DataLong(windowedObject.window().start()));
            result.put(END_FIELD, new DataLong(windowedObject.window().end()));
            result.put(START_TIME_FIELD, new DataString(windowedObject.window().startTime().toString()));
            result.put(END_TIME_FIELD, new DataString(windowedObject.window().endTime().toString()));
            result.put(KEY_FIELD, keyAsData);
            return result;
        }
        return nativeDataObjectMapper.toDataObject(object);
    }

    // This method makes expected data types compatible with the actual data that was created.
    // It does so by converting numbers to strings, and vice versa. It can convert complex data
    // objects like Enums, Lists and Structs too, recursively going through sub-elements if
    // necessary.
    public static DataObject makeCompatible(DataObject object, DataType type) {
        // Convert from Numbers to Strings
        if (type == DataString.DATATYPE) {
            if (object instanceof DataByte value) return new DataString("" + value.value());
            if (object instanceof DataShort value) return new DataString("" + value.value());
            if (object instanceof DataInteger value) return new DataString("" + value.value());
            if (object instanceof DataLong value) return new DataString("" + value.value());
            if (object instanceof DataFloat value) return new DataString("" + value.value());
            if (object instanceof DataDouble value) return new DataString("" + value.value());
        }

        // Convert from Strings to Numbers
        if (object instanceof DataString str) {
            if (type == DataByte.DATATYPE)
                return new DataByte(Byte.parseByte(str.value()));
            if (type == DataShort.DATATYPE)
                return new DataShort(Short.parseShort(str.value()));
            if (type == DataInteger.DATATYPE)
                return new DataInteger(Integer.parseInt(str.value()));
            if (type == DataLong.DATATYPE)
                return new DataLong(Long.parseLong(str.value()));
            if (type == DataFloat.DATATYPE)
                return new DataFloat(Float.parseFloat(str.value()));
            if (type == DataDouble.DATATYPE)
                return new DataDouble(Double.parseDouble(str.value()));
        }

        // Convert from Lists without a value type to Lists that do have a value type
        if (object instanceof DataList list && type instanceof ListType listType) {
            var valueType = listType.valueType();
            if (valueType == null || valueType == DataType.UNKNOWN) return object;

            // Create a new List with the give value type
            var result = new DataList(valueType);

            // Copy all list elements into the new list, possibly making sub-elements compatible
            for (var element : list) {
                result.add(makeCompatible(element, valueType));
            }

            // Return the List with made-compatible elements
            return result;
        }
        // Convert from schemaless Structs to Structs that do have a schema
        if (object instanceof DataStruct struct && type instanceof StructType structType) {
            var schema = structType.schema();
            if (schema == null) return object;

            // Create a new Struct with the given schema type
            var result = new DataStruct(structType);

            // Copy all struct fields into the new struct, possibly making sub-elements compatible
            for (var entry : struct.entrySet()) {
                var fieldName = entry.getKey();
                var fieldType = SchemaUtil.schemaToDataType(schema.field(fieldName).schema());
                result.put(entry.getKey(), makeCompatible(entry.getValue(), fieldType));
            }

            // Return the Struct with made-compatible fields
            return result;
        }

        // If no conversion was found suitable, then just return the object itself
        return object;
    }
}
