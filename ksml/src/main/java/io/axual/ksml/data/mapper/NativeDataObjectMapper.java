package io.axual.ksml.data.mapper;

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

import java.util.List;
import java.util.Map;

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataEnum;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.object.DataUnion;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.value.Tuple;
import io.axual.ksml.exception.KSMLExecutionException;

public class NativeDataObjectMapper extends BaseDataObjectMapper<Object> {
    @Override
    public DataObject toDataObject(DataType expected, Object value) {
        if (value == null) return new DataNull();
        if (value instanceof DataObject val) return val;
        if (value instanceof Boolean val) return new DataBoolean(val);
        if (value instanceof Byte val) return new DataByte(val);
        if (value instanceof Short val) return new DataShort(val);
        if (value instanceof Integer val) return new DataInteger(val);
        if (value instanceof Long val) return new DataLong(val);
        if (value instanceof Double val) return new DataDouble(val);
        if (value instanceof Float val) return new DataFloat(val);
        if (value instanceof byte[] val) return new DataBytes(val);
        if (value instanceof String val) return new DataString(val);
        if (value instanceof List<?> val) return toDataList((List<Object>) val);
        if (value instanceof Map<?, ?> val) return toDataStruct((Map<String, Object>) val, null);
        if (value instanceof Tuple<?> val) return toDataTuple((Tuple<Object>) val);
        throw new KSMLExecutionException("Can not convert to DataObject: " + value.getClass().getSimpleName());
    }

    @Override
    public Object fromDataObject(DataObject value) {
        if (value instanceof DataNull val) return val.value();

        if (value instanceof DataBoolean val) return val.value();

        if (value instanceof DataByte val) return val.value();
        if (value instanceof DataShort val) return val.value();
        if (value instanceof DataInteger val) return val.value();
        if (value instanceof DataLong val) return val.value();

        if (value instanceof DataDouble val) return val.value();
        if (value instanceof DataFloat val) return val.value();

        if (value instanceof DataBytes val) return val.value();

        if (value instanceof DataString val) return val.value();

        if (value instanceof DataEnum val) return val.value();
        if (value instanceof DataList val) return fromDataList(val);
        if (value instanceof DataStruct val) return fromDataStruct(val);
        if (value instanceof DataTuple val) return fromDataTuple(val);

        if (value instanceof DataUnion val) return val.value();

        throw new KSMLExecutionException("Can not convert DataObject to native dataType: " + value.getClass().getSimpleName());
    }
}
