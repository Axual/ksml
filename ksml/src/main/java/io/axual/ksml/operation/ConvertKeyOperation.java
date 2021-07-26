package io.axual.ksml.operation;

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

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.object.DataRecord;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.DataTypeAndNotation;
import io.axual.ksml.data.type.RecordType;

public class ConvertKeyOperation extends BaseOperation {
    private final DataTypeAndNotation targetTypeAndNotation;

    private static class KeyConverter implements KeyValueMapper<Object, Object, Object> {
        private final RecordType targetRecordType;

        public KeyConverter(DataType toType) {
            this.targetRecordType = toType instanceof RecordType ? (RecordType) toType : null;
        }

        @Override
        public Object apply(Object key, Object value) {
            if (targetRecordType == null) return key;
            var result = new DataRecord(targetRecordType.schema());
            result.putAll((DataRecord) key);
            return result;
        }
    }

    private final KeyConverter converter;
    private final NotationLibrary notationLibrary;

    public ConvertKeyOperation(String name, DataTypeAndNotation targetTypeAndNotation, NotationLibrary notationLibrary) {
        super(name);
        this.targetTypeAndNotation = targetTypeAndNotation;
        this.notationLibrary = notationLibrary;
        converter = new KeyConverter(this.targetTypeAndNotation.type);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.selectKey(converter, Named.as(name)),
                StreamDataType.of(targetTypeAndNotation.type, notationLibrary.get(targetTypeAndNotation.notation), true),
                input.valueType);
    }
}
