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


import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.data.type.WindowType;

public class CountOperation extends StoreOperation {
    private final Notation longNotation;

    public CountOperation(String name, String storeName, Notation longNotation) {
        super(name, storeName);
        this.longNotation = longNotation;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.groupedStream.count(Named.as(name), Materialized.as(storeName)),
                input.keyType,
                StreamDataType.of(DataLong.TYPE, input.valueType.notation, false));
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input) {
        return new KTableWrapper(
                (KTable) input.groupedTable.count(Named.as(name), Materialized.as(storeName)),
                input.keyType,
                StreamDataType.of(DataLong.TYPE, input.valueType.notation, false));
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.sessionWindowedKStream.count(Named.as(name), Materialized.as(storeName)),
                StreamDataType.of(new WindowType(input.keyType.type), input.keyType.notation, true),
                StreamDataType.of(DataLong.TYPE, input.valueType.notation, false));

//        final var sourceKeySerde = input.keyType.notation.getSerde(input.keyType.type, true);
//
//        final var countedStream = input.sessionWindowedKStream.count(
//                Named.as(storeName),
//                Materialized.<Object, Long, SessionStore<Bytes, byte[]>>as(storeName)
//                        .withKeySerde(sourceKeySerde)
//                        .withValueSerde(Serdes.Long())
//        );
//
//        return normalize(input, countedStream);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input) {
        return new KTableWrapper(
                (KTable) input.timeWindowedKStream.count(Named.as(name), Materialized.as(storeName)),
                StreamDataType.of(new WindowType(input.keyType.type), input.keyType.notation, true),
                StreamDataType.of(DataLong.TYPE, input.valueType.notation, false));

//        final var sourceKeySerde = input.keyType.notation.getSerde(input.keyType.type, true);
//
//        final var countedStream = input.timeWindowedKStream.count(
//                Named.as(storeName),
//                Materialized.<DataObject, Long, WindowStore<Bytes, byte[]>>as(storeName)
//                        .withKeySerde(sourceKeySerde)
//                        .withValueSerde(Serdes.Long())
//        );
//
//        return normalize(input, countedStream);
    }

//    private StreamWrapper normalize(StreamWrapper input, KTable<Windowed<DataObject>,Long> countTable) {
//        final var keyType = new RecordType(SchemaUtils.generateWindowSchema(input.getKeyType().type));
//        final var valueType = StandardType.LONG;
//        final var keySerde = input.getKeyType().notation.getSerde(keyType, true);
//        final var valueSerde = longNotation.getSerde(valueType, false);
//
//        final var normalizedStream = countTable.toStream().map(
//                (key, value) -> new KeyValue<DataObject, DataObject>(SchemaUtils.convertWindow(key), new DataLong(value))
//        );
//        final var tableExport = normalizedStream.toTable(
//                Named.as(storeName + "2"),
//                Materialized.<DataObject, DataObject, KeyValueStore<Bytes, byte[]>>as(storeName + "2")
//                        .withKeySerde(keySerde)
//                        .withValueSerde(valueSerde)
//        );
//
//        return new KTableWrapper(
//                tableExport,
//                StreamDataType.of(keyType, input.getKeyType().notation, true),
//                StreamDataType.of(valueType, longNotation, false));
//    }
}
