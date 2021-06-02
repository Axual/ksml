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

import io.axual.ksml.data.DataConverter;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.type.DataType;

public class ConvertKeyOperation extends BaseOperation {
    private static class KeyConverter extends DataConverter implements KeyValueMapper<Object, Object, Object> {
        public KeyConverter(DataType toType) {
            super(toType);
        }

        @Override
        public Object apply(Object key, Object value) {
            return convert(key);
        }
    }

    private final KeyConverter converter;

    public ConvertKeyOperation(String name, DataType toType) {
        super(name);
        converter = new KeyConverter(toType);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.selectKey(converter, Named.as(name)),
                StreamDataType.of(converter.toType, true),
                input.valueType);
    }
}
