package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.WindowedType;
import org.apache.kafka.streams.kstream.Windowed;

import static io.axual.ksml.dsl.WindowedSchema.*;

// KSML uses a generic policy that ALL data in streams is internally represented as DataObjects.
// However, Kafka Streams also dictates some types of its own, namely classes like Windowed and
// Long, which are both used in the count() operation. This leads to ClassCastExceptions if we
// define our stream types as KStream<DataObject,DataObject>, since Windows and Long cannot be
// cast to DataObject. Therefore, all formal stream types in the Java code are
// KStream<Object,Object> instead of KStream<DataObject,DataObject>. This mapper class ensures
// that any data dataType injected by Kafka Streams gets modified into a proper DataObject on
// the fly. When new data types pop up in Kafka Streams' generics, add the conversion to a
// DataObject to this method.
public class KafkaStreamsDataObjectMapper extends NativeDataObjectMapper {
    public KafkaStreamsDataObjectMapper(boolean includeTypeInfo) {
        super(includeTypeInfo);
    }

    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value instanceof Windowed<?> windowedObject) {
            // Convert a Windowed object into a struct with fields that contain the window fields.
            var keyAsData = toDataObject(windowedObject.key());
            var schema = generateWindowedSchema(new WindowedType(keyAsData.type()));
            var result = new DataStruct(schema);
            result.put(WINDOWED_SCHEMA_START_FIELD, new DataLong(windowedObject.window().start()));
            result.put(WINDOWED_SCHEMA_END_FIELD, new DataLong(windowedObject.window().end()));
            result.put(WINDOWED_SCHEMA_START_TIME_FIELD, new DataString(windowedObject.window().startTime().toString()));
            result.put(WINDOWED_SCHEMA_END_TIME_FIELD, new DataString(windowedObject.window().endTime().toString()));
            result.put(WINDOWED_SCHEMA_KEY_FIELD, keyAsData);
            return result;
        }
        return super.toDataObject(expected, value);
    }
}
