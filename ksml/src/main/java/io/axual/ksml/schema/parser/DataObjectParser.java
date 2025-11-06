package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;

public class DataObjectParser extends BaseParser<DataObject> {
    @Override
    public DataObject parse(ParseNode node) {
        if (node == null) return null;
        if (node.isNull()) return DataNull.INSTANCE;
        if (node.isBoolean()) return new DataBoolean(node.asBoolean());
        if (node.isInt()) return new DataInteger(node.asInt());
        if (node.isLong()) return new DataLong(node.asLong());
        if (node.isDouble()) return new DataDouble(node.asDouble());
        if (node.isString()) return new DataString(node.asString());
        throw new ParseException(node, "Can not parse value type: " + node.asString());
    }
}
