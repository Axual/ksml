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

import io.axual.ksml.data.mapper.NativeUserObjectMapper;
import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.base.WindowedType;
import io.axual.ksml.schema.SchemaUtil;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;
import static io.axual.ksml.schema.WindowedSchema.END_FIELD;
import static io.axual.ksml.schema.WindowedSchema.END_TIME_FIELD;
import static io.axual.ksml.schema.WindowedSchema.KEY_FIELD;
import static io.axual.ksml.schema.WindowedSchema.START_FIELD;
import static io.axual.ksml.schema.WindowedSchema.START_TIME_FIELD;

public class DataUtil {
    private static final NativeUserObjectMapper nativeUserObjectMapper = new NativeUserObjectMapper();

    private DataUtil() {
    }

    // KSML uses a generic policy that ALL data in streams is internally represented as UserObjects.
    // However Kafka Streams also dictates some types of its own, namely classes like Windowed and
    // Long, which are both used in the count() operation. This leads to ClassCastExceptions if we
    // define our stream types as KStream<UserObject,UserObject>, since Windows and Long cannot be
    // casted to UserObject. Therefore all formal stream types in the Java code are
    // KStream<Object,Object> instead of KStream<UserObject,UserObject>. The unify method ensures
    // that any data type injected by Kafka Streams gets modified into a proper UserObject on the
    // fly. When new data types pop up in Kafka Streams' generics, add the conversion to a
    // UserObject to this method.
    public static UserObject asUserObject(Object object) {
        if (object instanceof UserObject) return (UserObject) object;
        if (object instanceof Windowed<?>) return windowAsRecord((Windowed<?>) object);
        return nativeUserObjectMapper.toUserObject(DEFAULT_NOTATION, object);
    }

    // Convert a Windowed object into a data record with fields that contain the window fields.
    private static UserRecord windowAsRecord(Windowed<?> windowedObject) {
        var keyAsData = asUserObject(windowedObject.key());
        var schema = SchemaUtil.windowTypeToSchema(new WindowedType(keyAsData.type().type()));
        var result = new UserRecord(schema);
        result.put(START_FIELD, new UserLong(windowedObject.window().start()));
        result.put(END_FIELD, new UserLong(windowedObject.window().start()));
        result.put(START_TIME_FIELD, new UserString(windowedObject.window().startTime().toString()));
        result.put(END_TIME_FIELD, new UserString(windowedObject.window().endTime().toString()));
        result.put(KEY_FIELD, keyAsData);
        return result;
    }

}
