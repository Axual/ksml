package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.type.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringDataObjectMapperTest {
    private final StringDataObjectMapper mapper = new StringDataObjectMapper();

    @Test
    @DisplayName("toDataObject returns DataString when expected is DataString; preserves value including null")
    void toDataObjectReturnsDataStringOnlyWhenExpectedMatches() {
        var expectedType = DataString.DATATYPE;

        var helloWorldValue = "hello world";
        var dataObjectFromHello = mapper.toDataObject(expectedType, helloWorldValue);
        assertThat(dataObjectFromHello)
                .isInstanceOf(DataString.class)
                .extracting(o -> ((DataString) o).value())
                .isEqualTo(helloWorldValue);

        String nullStringValue = null;
        var dataObjectFromNull = mapper.toDataObject(expectedType, nullStringValue);
        assertThat(dataObjectFromNull)
                .isInstanceOf(DataString.class)
                .extracting(o -> ((DataString) o).value())
                .isNull();
    }

    @Test
    @DisplayName("toDataObject returns null when expected is null or not DataString")
    void toDataObjectReturnsNullForNonMatchingExpectedType() {
        var someString = "ignored";

        DataType expectedNull = null;
        var resultWithNullExpected = mapper.toDataObject(expectedNull, someString);
        assertThat(resultWithNullExpected).isNull();

        DataType mismatchingExpected = DataInteger.DATATYPE;
        var resultWithMismatchingExpected = mapper.toDataObject(mismatchingExpected, someString);
        assertThat(resultWithMismatchingExpected).isNull();
    }

    @Test
    @DisplayName("fromDataObject unwraps DataString value including null contents")
    void fromDataObjectUnwrapsDataString() {
        var wrappedText = new DataString("wrapped");
        var unwrapped = mapper.fromDataObject(wrappedText);
        assertThat(unwrapped).isEqualTo("wrapped");

        var wrappedNull = new DataString((String) null);
        var unwrappedNull = mapper.fromDataObject(wrappedNull);
        assertThat(unwrappedNull).isNull();
    }

    @Test
    @DisplayName("fromDataObject returns null for non-DataString values and for null input")
    void fromDataObjectReturnsNullForUnsupportedOrNull() {
        var integerObject = new DataInteger(42);
        assertThat(mapper.fromDataObject(integerObject)).isNull();

        DataObject nullInput = null;
        assertThat(mapper.fromDataObject(nullInput)).isNull();
    }
}
