package io.axual.ksml.type;

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

import java.util.Map;

public class StandardType {
    public static final SimpleType BOOLEAN = new SimpleType(Boolean.class);
    public static final SimpleType BYTES = new SimpleType(byte[].class);
    public static final SimpleType DOUBLE = new SimpleType(Double.class);
    public static final SimpleType FLOAT = new SimpleType(Float.class);
    public static final SimpleType INTEGER = new SimpleType(Integer.class);
    public static final SimpleType LONG = new SimpleType(Long.class);
    public static final SimpleType STRING = new SimpleType(String.class);

    public static final ComplexType JSON = new ComplexType(Map.class, new DataType[]{StandardType.STRING, DataType.UNKNOWN});

    private StandardType() {
    }
}
