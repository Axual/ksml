package io.axual.ksml.serde;

import org.apache.kafka.common.serialization.Deserializer;

import io.axual.ksml.data.value.Null;
import io.axual.ksml.exception.KSMLExecutionException;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

public class NullDeserializer implements Deserializer<Object> {
    @Override
    public Object deserialize(String s, byte[] bytes) {
        if (bytes == null || bytes.length == 0) return Null.NULL;
        throw new KSMLExecutionException("Can only deserialize empty byte arrays as DataNull");
    }
}
