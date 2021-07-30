package io.axual.ksml.stream;

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



import io.axual.ksml.generator.StreamDataType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BaseStreamWrapper implements StreamWrapper {
    public final StreamDataType keyType;
    public final StreamDataType valueType;

    public String toString() {
        String type = getClass().getSimpleName();
        if (type.toLowerCase().endsWith("wrapper")) {
            type = type.substring(0, type.length() - 7);
        }
        return type + "[key=" + keyType + ", value=" + valueType + "]";
    }

    @Override
    public StreamDataType keyType() {
        return keyType;
    }

    @Override
    public StreamDataType valueType() {
        return valueType;
    }
}
