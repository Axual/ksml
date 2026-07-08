package io.axual.ksml.schema.parser;

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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DataObjectParserTest {

    private final DataObjectParser parser = new DataObjectParser();

    private static ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    @Test
    void returnsNullForNullNode() {
        assertThat(parser.parse(null)).isNull();
    }

    @Test
    void parsesNull() throws Exception {
        assertThat(parser.parse(nodeOf("null"))).isEqualTo(DataNull.INSTANCE);
    }

    @Test
    void parsesBoolean() throws Exception {
        assertThat(parser.parse(nodeOf("true"))).isEqualTo(new DataBoolean(true));
    }

    @Test
    void parsesInteger() throws Exception {
        assertThat(parser.parse(nodeOf("42"))).isEqualTo(new DataInteger(42));
    }

    @Test
    void parsesLong() throws Exception {
        assertThat(parser.parse(nodeOf("2147483648"))).isEqualTo(new DataLong(2147483648L));
    }

    @Test
    void parsesDouble() throws Exception {
        assertThat(parser.parse(nodeOf("3.14"))).isEqualTo(new DataDouble(3.14));
    }

    @Test
    void parsesString() throws Exception {
        assertThat(parser.parse(nodeOf("hello"))).isEqualTo(new DataString("hello"));
    }

    @Test
    void rejectsUnsupportedType() {
        assertThatThrownBy(() -> parser.parse(nodeOf("[1, 2, 3]")))
                .isInstanceOf(ParseException.class);
    }
}
