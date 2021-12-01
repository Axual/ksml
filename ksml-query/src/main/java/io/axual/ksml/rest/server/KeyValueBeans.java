package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
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

import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.rest.data.WindowedData;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * POJO representing store data elements
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class KeyValueBeans {
    private final List<KeyValueBean> elements = new ArrayList<>();

    public List<KeyValueBean> elements() {
        return elements;
    }

    public KeyValueBeans add(Object key, Object value) {
        if (key instanceof Windowed) {
            Windowed<Object> windowedKey = (Windowed<Object>) key;
            elements.add(new KeyValueBean(new WindowedData(windowedKey), value));
        } else {
            elements.add(new KeyValueBean(key, value));
        }
        return this;
    }

    public KeyValueBeans add(KeyValueBean element) {
        elements.add(new KeyValueBean(element.getKey(), element.getValue()));
        return this;
    }

    public KeyValueBeans add(KeyValueBeans otherBeans) {
        elements.addAll(otherBeans.elements);
        return this;
    }

    @Override
    public String toString() {
        return "StoreData{" + "elements=" + elements + '}';
    }

    public static KeyValueBeans EMPTY() {
        return new KeyValueBeans();
    }
}
