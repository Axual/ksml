package io.axual.ksml.data.parser.schema;

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

import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.DataValue;

public class DataValueParser extends BaseParser<DataValue> {
    @Override
    public DataValue parse(ParseNode node) {
        if (node == null) return null;
        if (node.isNull()) return new DataValue(null);
        if (node.isBoolean()) return new DataValue(node.asBoolean());
        if (node.isInt()) return new DataValue(node.asInt());
        if (node.isLong()) return new DataValue(node.asLong());
        if (node.isDouble()) return new DataValue(node.asDouble());
        if (node.isString()) return new DataValue(node.asString());
        throw new ParseException(node, "Can not parse value type: " + node.asString());
    }
}
