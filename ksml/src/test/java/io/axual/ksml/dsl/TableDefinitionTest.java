package io.axual.ksml.dsl;

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

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;

import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.notation.BinaryNotation;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.stream.KTableWrapper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TableDefinitionTest {

    @Mock
    private StreamsBuilder builder;

    private final NotationLibrary notationLibrary = new NotationLibrary(new HashMap<>());

    @Mock
    Notation mockNotation;

    @Test
    void testTableDefinition() {
        notationLibrary.put(BinaryNotation.NOTATION_NAME, mockNotation);

        // given a TableDefinition
        var tableDefinition = new TableDefinition("topic", "string", "string", false, null);

        // when it adds itself to Builder
        var streamWrapper = tableDefinition.addToBuilder(builder, "name", notationLibrary);

        // it adds a ktable to the builder with key and value dataType, and returns a KTableWrapper instance
        final var stringType = UserTypeParser.parse("string");
        verify(mockNotation).getSerde(stringType.dataType(), true);
        verify(mockNotation).getSerde(stringType.dataType(), false);

        verify(builder).table(eq("topic"), isA(Consumed.class));
        assertThat(streamWrapper, instanceOf(KTableWrapper.class));
    }
}
