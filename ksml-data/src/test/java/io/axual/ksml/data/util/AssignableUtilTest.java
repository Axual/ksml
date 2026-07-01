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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.UnionType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AssignableUtilTest {

    private static final UnionType UNION = new UnionType(new UnionType.Member(DataInteger.DATATYPE));

    @Test
    @DisplayName("Every factory produces a not-assignable Assignable")
    void factoriesProduceNotAssignable() {
        assertThat(AssignableUtil.fieldNotAssignable("f", "t1", 1, "t2", 2).isNotAssignable()).isTrue();
        assertThat(AssignableUtil.schemaMismatch(DataSchema.INTEGER_SCHEMA, DataSchema.STRING_SCHEMA).isNotAssignable()).isTrue();
        assertThat(AssignableUtil.typeMismatch(DataInteger.DATATYPE, DataString.DATATYPE).isNotAssignable()).isTrue();
        assertThat(AssignableUtil.unionNotAssignableFromMember(UNION, new UnionType.Member(DataString.DATATYPE)).isNotAssignable()).isTrue();
        assertThat(AssignableUtil.unionNotAssignableFromType(UNION, DataString.DATATYPE).isNotAssignable()).isTrue();
        assertThat(AssignableUtil.unionNotAssignableFromValue(UNION, "some-value").isNotAssignable()).isTrue();
    }

    @Test
    @DisplayName("The cause-carrying fieldNotAssignable variant keeps its cause")
    void fieldNotAssignableWithCause() {
        final var cause = Assignable.notAssignable("inner");

        assertThat(AssignableUtil.fieldNotAssignable("f", "t1", 1, "t2", 2, cause).cause()).isSameAs(cause);
    }

    @Test
    @DisplayName("Messages mention the relevant field name")
    void messagesAreInformative() {
        assertThat(AssignableUtil.fieldNotAssignable("myField", "t1", 1, "t2", 2).message()).contains("myField");
    }
}
