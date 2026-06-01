package io.axual.ksml.data.notation.soap;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link SoapNotation} covering serde-type selection.
 *
 * <p>Regression for <a href="https://github.com/Axual/ksml/issues/354">issue #354</a>:
 * the notation declares {@link SoapNotation#DEFAULT_TYPE} as a {@link StructType}, but its
 * {@code serde(...)} only accepts {@link MapType}. As a consequence every pipeline that
 * uses {@code valueType: soap} or {@code resultType: soap} blows up at serde construction
 * time with {@code "soap serde not available for data type: ..."}.
 *
 * <p>The first test asserts the contract a user expects when they write {@code valueType:
 * soap}: the notation's own default type must be serializable by the notation. It is
 * expected to fail until the fix described in the issue (also accept {@code StructType})
 * lands.
 */
@DisplayName("SoapNotation - serde selection (regression for #354)")
class SoapNotationTest {

    @Test
    @DisplayName("Serde is provided for DEFAULT_TYPE (StructType) - fails until #354 is fixed")
    void serdeForDefaultTypeIsProvided() {
        var notation = new SoapNotation(new NotationContext(SoapNotation.NOTATION_NAME));

        // DEFAULT_TYPE is a StructType (see SoapNotation line 38). The serde method
        // currently only accepts MapType, so this throws today. After the fix this should
        // return a Serde just like the MapType branch does.
        Serde<Object> serde = notation.serde(SoapNotation.DEFAULT_TYPE, false);

        assertThat(serde)
                .as("SoapNotation.serde(DEFAULT_TYPE) must succeed; otherwise valueType: soap is unusable")
                .isNotNull();
    }

    @Test
    @DisplayName("Serde is provided for MapType (the currently-working path)")
    void serdeForMapTypeIsProvided() {
        var notation = new SoapNotation(new NotationContext(SoapNotation.NOTATION_NAME));

        // MapType is the only type the current serde() guard accepts. This test passes
        // today and is here to document the asymmetry the bug fix is meant to close.
        assertThat(notation.serde(new MapType(), false)).isNotNull();
    }

    @Test
    @DisplayName("Serde rejects unrelated types with a clear DataException")
    void serdeForUnrelatedTypeThrows() {
        var notation = new SoapNotation(new NotationContext(SoapNotation.NOTATION_NAME));

        // Types that are neither Map nor Struct should still be rejected after the fix
        // - this asserts the rejection wording is consistent so the fix doesn't
        // accidentally loosen the guard further.
        assertThatThrownBy(() -> notation.serde(io.axual.ksml.data.object.DataString.DATATYPE, true))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("soap serde not available for data type:");
    }
}
