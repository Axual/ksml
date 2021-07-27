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

import io.axual.ksml.data.mapper.NativeDataMapper;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.schema.WindowSchema;

import static io.axual.ksml.schema.WindowSchema.END_FIELD;
import static io.axual.ksml.schema.WindowSchema.END_TIME_FIELD;
import static io.axual.ksml.schema.WindowSchema.KEY_FIELD;
import static io.axual.ksml.schema.WindowSchema.START_FIELD;
import static io.axual.ksml.schema.WindowSchema.START_TIME_FIELD;

public class DataUtil {
    private static final NativeDataMapper nativeDataMapper = new NativeDataMapper();

    private DataUtil() {
    }

    // KSML uses a generic policy that ALL data in streams is internally represented as DataObjects.
    // However Kafka Streams also dictates some types of its own, namely classes like Windowed and
    // Long, which are both used in the count() operation. This leads to ClassCastExceptions if we
    // define our stream types as KStream<DataObject,DataObject>, since Windows and Long cannot be
    // casted to DataObject. Therefore all formal stream types in the Java code are
    // KStream<Object,Object> instead of KStream<DataObject,DataObject>. The unify method ensures
    // that any data type injected by Kafka Streams gets modified into a proper DataObject on the
    // fly. When new data types pop up in Kafka Streams' generics, add the conversion to a
    // DataObject to this method.
    public static DataObject asData(Object object) {
        if (object instanceof DataObject) return (DataObject) object;
        if (object instanceof Windowed<?> && ((Windowed<?>) object).key() instanceof DataObject)
            return convertWindow((Windowed<?>) object);
        return nativeDataMapper.toDataObject(object);
    }

    // Convert a Windowed object into a data record with fields that contain the window fields.
    private static DataRecord convertWindow(Windowed<?> windowedObject) {
        var keyAsData = asData(windowedObject.key());
        var schema = WindowSchema.generateWindowSchema(keyAsData.type());
        var result = new DataRecord(schema);
        result.put(START_FIELD, new DataLong(windowedObject.window().start()));
        result.put(END_FIELD, new DataLong(windowedObject.window().start()));
        result.put(START_TIME_FIELD, new DataString(windowedObject.window().startTime().toString()));
        result.put(END_TIME_FIELD, new DataString(windowedObject.window().endTime().toString()));
        result.put(KEY_FIELD, keyAsData);
        return result;
    }
}
