package io.axual.ksml.data.type;

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


import org.apache.kafka.streams.kstream.Window;

import io.axual.ksml.exception.KSMLTopologyException;

public class WindowType extends ComplexType {
    public WindowType(DataType type) {
        super(Window.class, new DataType[]{type});
    }

    public DataType getWindowedType() {
        return subTypes[0];
    }

    @Override
    public String schemaName() {
        return schemaName("Window");
    }

    public static WindowType createFrom(DataType type) {
        if (type instanceof ComplexType) {
            ComplexType outerType = (ComplexType) type;
            if (Window.class.isAssignableFrom(outerType.type) && outerType.subTypes.length == 1) {
                return new WindowType(outerType.subTypes[0]);
            }
        }
        throw new KSMLTopologyException("Could not convert type to Window: " + type);
    }
}
