package io.axual.ksml.data.object;

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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.Tuple;
import io.axual.ksml.data.type.TupleType;

public class DataTuple extends Tuple<DataObject> implements DataObject {
    private final DataType type;

    public DataTuple(DataObject... elements) {
        super(elements);
        DataType[] types = new DataType[elements.length];
        for (int index = 0; index < elements.length; index++) {
            types[index] = elements[index].type();
        }
        this.type = new TupleType(types);
    }

    @Override
    public DataType type() {
        return type;
    }
}
