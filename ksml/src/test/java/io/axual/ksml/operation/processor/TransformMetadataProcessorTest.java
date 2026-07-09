package io.axual.ksml.operation.processor;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.type.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.axual.ksml.operation.processor.PeekProcessorTest.fixedKeyRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TransformMetadataProcessorTest {

    private static final String[] NO_STORES = new String[0];

    @Mock
    private FixedKeyProcessorContext<Object, Object> context;

    @Test
    void appliesNewTimestampAndHeaders() {
        final var headers = new RecordHeaders();
        headers.add("h", "v".getBytes());
        final var metadata = new RecordMetadata(9999L, headers);
        final var processor = new TransformMetadataProcessor("transformMetadata", (stores, rec) -> metadata, NO_STORES);
        processor.init(context);

        processor.process(fixedKeyRecord("key", "value"));

        final ArgumentCaptor<FixedKeyRecord<Object, Object>> captor = ArgumentCaptor.captor();
        verify(context).forward(captor.capture());
        final var forwarded = captor.getValue();
        assertThat(forwarded.timestamp()).isEqualTo(9999L);
        assertThat(forwarded.headers().lastHeader("h").value()).isEqualTo("v".getBytes());
    }

    @Test
    void forwardsUnchangedWhenMetadataFieldsAreNull() {
        final var metadata = new RecordMetadata(null, null);
        final var processor = new TransformMetadataProcessor("transformMetadata", (stores, rec) -> metadata, NO_STORES);
        processor.init(context);

        final var rec = fixedKeyRecord("key", "value");
        processor.process(rec);

        verify(context).forward(rec);
    }
}
