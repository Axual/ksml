package io.axual.ksml.parser;

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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.axual.ksml.exception.KSMLExecutionException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This class helps to track the location of syntax/parsing errors in the YAML by maintaining a
// list of direct parents to a certain JsonNode.
public class YamlNode {
    private final JsonNode node;
    private final YamlNode parent;
    private final String name;

    public static YamlNode fromRoot(JsonNode root, String name) {
        return new YamlNode(null, root, name);
    }

    private YamlNode(YamlNode parent, JsonNode child, String name) {
        this.parent = parent;
        this.node = child;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private String getPathInternal(String separator) {
        return (parent != null ? parent.getPathInternal(separator) + separator : "") + name;
    }

    public String getDottedName() {
        return getPathInternal(".");
    }

    public String getLongName() {
        return getPathInternal("_");
    }

    @Override
    public String toString() {
        return getPathInternal("->");
    }

    public YamlNode get(String childName, String displayName) {
        JsonNode child = node.get(childName);
        if (child != null) {
            return new YamlNode(this, child, displayName);
        }
        return null;
    }

    public YamlNode get(String childName) {
        return get(childName, childName);
    }

    public YamlNode appendName(String appendName) {
        // Trick to append "appendName" to the named output, without traversing to an actual child JsonNode
        return new YamlNode(this, node, appendName);
    }

    public List<YamlNode> getChildren() {
        List<YamlNode> result = new ArrayList<>();
        Set<JsonNode> seen = new HashSet<>();
        Iterator<Map.Entry<String, JsonNode>> fieldIterator = node.fields();
        while (fieldIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldIterator.next();
            seen.add(field.getValue());
            result.add(new YamlNode(this, field.getValue(), field.getKey()));
        }

        Iterator<JsonNode> elementIterator = node.elements();
        int index = 1;
        while (elementIterator.hasNext()) {
            JsonNode element = elementIterator.next();
            if (!seen.contains(element)) {
                result.add(new YamlNode(this, element, "" + index++));
            }
        }
        return result;
    }

    public boolean isNull() {
        return node.isNull();
    }

    public boolean isBoolean() {
        return node.isBoolean();
    }

    public boolean asBoolean() {
        return node.asBoolean();
    }

    public boolean isShort() {
        return node.isShort();
    }

    public short asShort() {
        return node.numberValue().shortValue();
    }

    public boolean isInt() {
        return node.isInt();
    }

    public int asInt() {
        return node.intValue();
    }

    public boolean isLong() {
        return node.isLong();
    }

    public long asLong() {
        return node.longValue();
    }

    public boolean isDouble() {
        return node.isDouble();
    }

    public double asDouble() {
        return node.doubleValue();
    }

    public boolean isFloat() {
        return node.isFloat();
    }

    public float asFloat() {
        return node.floatValue();
    }

    public boolean isBytes() {
        return node.isBinary();
    }

    public byte[] asBytes() {
        try {
            return node.binaryValue();
        } catch (Exception e) {
            throw new KSMLExecutionException("Can not convert node to bytes", e);
        }
    }

    public boolean isString() {
        return node.isTextual();
    }

    public String asString() {
        return node.asText();
    }

    public boolean isArray() {
        return node.isArray();
    }

    public boolean isObject() {
        return node.isObject();
    }

    public boolean childIsText(String childName) {
        return node.get(childName) instanceof TextNode;
    }
}
