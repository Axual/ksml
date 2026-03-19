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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.value.Struct;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonDict;
import io.axual.ksml.python.PythonNativeMapper;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;

public class ProxyUtil {
    private static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();

    private ProxyUtil() {
    }

    public static Object resultFrom(ValueAndTimestamp<Object> vat) {
        if (vat == null) return null;
        final var converted = new Struct<>();
        converted.put("value", toPython(vat.value()));
        converted.put("timestamp", toPython(vat.timestamp()));
        return new PythonDict(converted);
    }

    public static Object resultFrom(VersionedRecord<Object> vr) {
        if (vr == null) return null;
        final var converted = new Struct<>();
        converted.put("value", toPython(vr.value()));
        converted.put("timestamp", toPython(vr.timestamp()));
        vr.validTo().ifPresent(validTo -> converted.put("validTo", toPython(validTo)));
        return new PythonDict(converted);
    }

    public static Object toPython(Object object) {
        if (object == null) return null;
        if (object instanceof DataObject dataObject)
            return DATA_OBJECT_MAPPER.fromDataObject(dataObject);
        return NATIVE_MAPPER.toPython(object);
    }
}
