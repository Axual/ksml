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
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.schema.mapper.WindowedSchemaMapper;

import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.END_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.END_TIME_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.KEY_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.START_FIELD;
import static io.axual.ksml.schema.mapper.WindowedSchemaMapper.START_TIME_FIELD;

public class DataUtil {
    private static final NativeDataObjectMapper nativeUserObjectMapper = new NativeDataObjectMapper();
    private static final WindowedSchemaMapper windowedSchemaMapper = new WindowedSchemaMapper();

    private DataUtil() {
    }

    // KSML uses a generic policy that ALL data in streams is internally represented as UserObjects.
    // However Kafka Streams also dictates some types of its own, namely classes like Windowed and
    // Long, which are both used in the count() operation. This leads to ClassCastExceptions if we
    // define our stream types as KStream<UserObject,UserObject>, since Windows and Long cannot be
    // casted to UserObject. Therefore all formal stream types in the Java code are
    // KStream<Object,Object> instead of KStream<UserObject,UserObject>. The unify method ensures
    // that any data dataType injected by Kafka Streams gets modified into a proper UserObject on the
    // fly. When new data types pop up in Kafka Streams' generics, add the conversion to a
    // UserObject to this method.
    public static DataObject asUserObject(Object object) {
        if (object instanceof DataObject dataObject) return dataObject;
        if (object instanceof Windowed<?> windowedObject) {
            // Convert a Windowed object into a data record with fields that contain the window fields.
            var keyAsData = asUserObject(windowedObject.key());
            var schema = windowedSchemaMapper.toDataSchema(new WindowedType(keyAsData.type()));
            var result = new DataRecord(new RecordType(schema));
            result.put(START_FIELD, new DataLong(windowedObject.window().start()));
            result.put(END_FIELD, new DataLong(windowedObject.window().end()));
            result.put(START_TIME_FIELD, new DataString(windowedObject.window().startTime().toString()));
            result.put(END_TIME_FIELD, new DataString(windowedObject.window().endTime().toString()));
            result.put(KEY_FIELD, keyAsData);
            return result;
        }
        return nativeUserObjectMapper.toDataObject(object);
    }
}
