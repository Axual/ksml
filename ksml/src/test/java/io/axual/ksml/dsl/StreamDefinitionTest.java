package io.axual.ksml.dsl;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.notation.binary.BinaryNotation;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.stream.KStreamWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StreamDefinitionTest {

    @Mock
    private StreamsBuilder builder;

    @Mock
    Notation mockNotation;

    @Test
    void testStreamDefinition() {
        NotationLibrary.register(BinaryNotation.NOTATION_NAME, mockNotation);

        // given a StreamDefinition
        var streamDefinition = new StreamDefinition("topic", "string", "string");
        var resources = new TopologyResources("test");

        var context = new TopologyBuildContext(builder, resources);
        // when it adds itself to Builder
        var streamWrapper = context.getStreamWrapper(streamDefinition);

        // it adds a ktable to the builder with key and value dataType, and returns a KTableWrapper instance
        final var stringType = UserTypeParser.parse("string");
        verify(mockNotation).serde(stringType.dataType(), true);
        verify(mockNotation).serde(stringType.dataType(), false);

        verify(builder).stream(eq("topic"), isA(Consumed.class));
        assertThat(streamWrapper, instanceOf(KStreamWrapper.class));
    }
}
