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

import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.data.type.user.UserRecordType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.util.DataUtil;

public class ConvertKeyOperation extends BaseOperation {
    private static class KeyConverter implements KeyValueMapper<Object, Object, Object> {
        private final UserRecordType targetRecordType;

        public KeyConverter(DataType toType) {
            this.targetRecordType = toType instanceof UserRecordType ? (UserRecordType) toType : null;
        }

        @Override
        public Object apply(Object key, Object value) {
            var keyAsData = DataUtil.asUserObject(key);
            if (targetRecordType == null) return keyAsData;
            var result = new UserRecord(targetRecordType.schema());
            result.putAll((UserRecord) keyAsData);
            return result;
        }
    }

    private final UserType targetType;
    private final KeyConverter converter;

    public ConvertKeyOperation(OperationConfig config, DataType targetType, Notation targetNotation) {
        super(config);
        converter = new KeyConverter(targetType);
        this.targetType = converter.targetRecordType != null ? converter.targetRecordType : new StaticUserType(targetType, targetNotation.name());
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.selectKey(converter, Named.as(name)),
                streamDataTypeOf(targetType, true),
                input.valueType());
    }
}
