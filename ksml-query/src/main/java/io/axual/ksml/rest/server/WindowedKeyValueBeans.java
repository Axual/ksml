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

import org.apache.kafka.streams.kstream.Window;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * POJO representing store data elements
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class WindowedKeyValueBeans {
    private final List<WindowedKeyValueBean> elements = new ArrayList<>();

    public List<WindowedKeyValueBean> elements() {
        return elements;
    }

    public WindowedKeyValueBeans add(Window window, Object key, Object value) {
        elements.add(new WindowedKeyValueBean(window.start(), window.end(), key, value));
        return this;
    }

    public WindowedKeyValueBeans add(WindowedKeyValueBean element) {
        elements.add(new WindowedKeyValueBean(element.getWindow().getStart(), element.getWindow().getEnd(), element.getKey(), element.getValue()));
        return this;
    }

    public WindowedKeyValueBeans add(WindowedKeyValueBeans otherBeans) {
        elements.addAll(otherBeans.elements);
        return this;
    }

    @Override
    public String toString() {
        return "StoreData{" + "elements=" + elements + '}';
    }
}
