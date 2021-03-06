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

import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.axual.ksml.data.object.user.UserRecord;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.user.UserRecordType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.util.DataUtil;

public class ConvertValueOperation extends BaseOperation {
    private final UserType targetType;
    private final ValueConverter converter;

    private static class ValueConverter implements ValueMapper<Object, Object> {
        private final UserRecordType targetRecordType;

        public ValueConverter(DataType toType) {
            this.targetRecordType = toType instanceof UserRecordType ? (UserRecordType) toType : null;
        }

        @Override
        public Object apply(Object value) {
            var valueAsData = DataUtil.asUserObject(value);
            if (targetRecordType == null) return valueAsData;
            var result = new UserRecord(targetRecordType.schema());
            result.putAll((UserRecord) value);
            return result;
        }
    }

    public ConvertValueOperation(OperationConfig config, UserType targetType) {
        super(config);
        this.targetType = targetType;
        converter = new ValueConverter(this.targetType.type());
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.mapValues(converter, Named.as(name)),
                input.keyType(),
                streamDataTypeOf(targetType, false));
    }
}
