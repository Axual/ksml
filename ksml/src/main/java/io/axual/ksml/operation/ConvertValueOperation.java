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

import io.axual.ksml.data.DataConverter;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.type.DataType;

public class ConvertValueOperation extends BaseOperation {
    private static class ValueConverter extends DataConverter implements ValueMapper<Object, Object> {
        public ValueConverter(DataType toType) {
            super(toType);
        }

        @Override
        public Object apply(Object value) {
            return convert(value);
        }
    }

    private final ValueConverter converter;

    public ConvertValueOperation(String name, DataType toType) {
        super(name);
        converter = new ValueConverter(toType);
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        return new KStreamWrapper(
                input.stream.mapValues(converter, Named.as(name)),
                input.keyType,
                StreamDataType.of(converter.toType, false));
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        return new KTableWrapper(
                input.table.mapValues(converter, Named.as(name)),
                input.keyType,
                StreamDataType.of(converter.toType, false));
    }
}
