package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EqualUtilTest {

    @Test
    @DisplayName("Every factory produces a not-equal Equality with an explanatory message")
    void factoriesProduceNotEqual() {
        assertThat(EqualUtil.otherIsNull(new DataInteger(1)).isNotEqual()).isTrue();
        assertThat(EqualUtil.containerClassNotEqual(String.class, Integer.class).isNotEqual()).isTrue();
        assertThat(EqualUtil.objectNotEqual(new DataInteger(1), new DataInteger(2)).isNotEqual()).isTrue();
        assertThat(EqualUtil.typeNotEqual(DataInteger.DATATYPE, DataLong.DATATYPE, Equality.notEqual("cause")).isNotEqual()).isTrue();
        assertThat(EqualUtil.fieldNotEqual("field", new DataInteger(1), 1, new DataInteger(2), 2).isNotEqual()).isTrue();
    }

    @Test
    @DisplayName("The cause-carrying variants keep their underlying cause")
    void variantsCarryCause() {
        final var cause = Equality.notEqual("inner");

        assertThat(EqualUtil.objectNotEqual(new DataInteger(1), new DataInteger(2), cause).cause()).isSameAs(cause);
        assertThat(EqualUtil.fieldNotEqual("field", new DataInteger(1), 1, new DataInteger(2), 2, cause).cause()).isSameAs(cause);
    }

    @Test
    @DisplayName("Messages mention the relevant detail (class names / field name)")
    void messagesAreInformative() {
        assertThat(EqualUtil.containerClassNotEqual(String.class, Integer.class).message()).contains("String", "Integer");
        assertThat(EqualUtil.fieldNotEqual("myField", new DataInteger(1), 1, new DataInteger(2), 2).message()).contains("myField");
    }
}
