package io.axual.ksml.proxy.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonDict;
import io.axual.ksml.python.PythonNativeMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;

public class ProxyUtil {
    private static final String KEY_FIELD = "key";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String VALID_TO_FIELD = "validTo";
    private static final String VALUE_FIELD = "value";
    private static final DataObjectFlattener FLATTENER = new DataObjectFlattener();
    private static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();

    private ProxyUtil() {
    }

    public static Object resultFrom(ValueAndTimestamp<Object> vat) {
        if (vat == null) return null;
        final var converted = new Struct<>();
        converted.put(VALUE_FIELD, toPython(vat.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vat.timestamp()));
        return new PythonDict(converted);
    }

    public static Object resultFrom(VersionedRecord<Object> vr) {
        if (vr == null) return null;
        final var converted = new Struct<>();
        converted.put(VALID_TO_FIELD, toPython(vr.value()));
        converted.put(TIMESTAMP_FIELD, toPython(vr.timestamp()));
        vr.validTo().ifPresent(validTo -> converted.put(VALID_TO_FIELD, toPython(validTo)));
        return new PythonDict(converted);
    }

    public static Object resultFrom(KeyValue<?, ?> kv) {
        if (kv == null) return null;
        final var converted = new Struct<>();
        converted.put(KEY_FIELD, toPython(kv.key));
        converted.put(VALUE_FIELD, toPython(kv.value));
        return new PythonDict(converted);
    }

    public static Object toPython(Object object) {
        if (object == null) return null;
        if (object instanceof Windowed<?> windowed)
            object = FLATTENER.toDataObject(windowed);
        if (object instanceof DataObject dataObject)
            return DATA_OBJECT_MAPPER.fromDataObject(dataObject);
        return NATIVE_MAPPER.toPython(object);
    }
}
