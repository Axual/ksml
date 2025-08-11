package io.axual.ksml.data.object;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_ALL_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_NO_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.EXTERNAL_TOP_SCHEMA;
import static io.axual.ksml.data.object.DataObject.Printer.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class DataBytesTest {

    @Test
    @DisplayName("toString across printers prints hex bytes and schema prefix only for TOP/ALL; null prints without prefix for INTERNAL/NO_SCHEMA")
    void toStringPrinterModes() {
        var bytes = new DataBytes(new byte[]{0x00, 0x0A, (byte) 0xFF});
        assertThat(bytes.toString(INTERNAL)).isEqualTo("[00, 0A, FF]");
        assertThat(bytes.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("[00, 0A, FF]");
        assertThat(bytes.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("bytes: [00, 0A, FF]");
        assertThat(bytes.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("bytes: [00, 0A, FF]");

        var nul = new DataBytes();
        assertThat(nul.toString(INTERNAL)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_NO_SCHEMA)).isEqualTo("null");
        assertThat(nul.toString(EXTERNAL_TOP_SCHEMA)).isEqualTo("bytes: null");
        assertThat(nul.toString(EXTERNAL_ALL_SCHEMA)).isEqualTo("bytes: null");
    }

    @Test
    @DisplayName("Constructor defensively copies input array")
    void defensiveCopy() {
        byte[] src = new byte[]{1, 2, 3};
        var db = new DataBytes(src);
        // Mutate source; representation must not change
        src[1] = 42;
        assertThat(db.toString(INTERNAL)).isEqualTo("[01, 02, 03]");
    }

    @Test
    @DisplayName("Equality uses array reference equality (due to DataPrimitive); separate instances not equal even with same content")
    void equalsSemantics() {
        var a = new DataBytes(new byte[]{1, 2});
        var b = new DataBytes(new byte[]{1, 2});
        assertThat(a)
                .isEqualTo(a)
                .isNotEqualTo(b)
                .isNotEqualTo(null);
    }
}
