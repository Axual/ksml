package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class DataBytesTest {

    @Test
    void testDefaultValueConstructor() {
        DataBytes dataBytes = new DataBytes();
        assertArrayEquals(null, dataBytes.value());
    }

    @Test
    void testValueConstructor() {
        DataBytes dataBytes = new DataBytes(new byte[]{1, 2, 3});
        assertArrayEquals(new byte[]{1, 2, 3}, dataBytes.value());
    }
}
