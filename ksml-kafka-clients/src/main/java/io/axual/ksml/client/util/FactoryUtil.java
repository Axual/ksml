package io.axual.ksml.client.util;

/*-
 * ========================LICENSE_START=================================
 * axual-common
 * %%
 * Copyright (C) 2020 Axual B.V.
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

import io.axual.ksml.client.exception.ClientException;
import org.apache.kafka.common.utils.Utils;

public class FactoryUtil {
    private FactoryUtil() {
    }

    public static <T> T create(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ClientException("Could not instantiate object of type " + clazz.getName());
        }
    }

    public static <T> T create(final String className, Class<T> clazz) {
        if (className == null || className.isEmpty()) {
            throw new ClientException("No " + clazz.getName() + " class passed");
        }

        try {
            return Utils.newInstance(className, clazz);
        } catch (ClassNotFoundException e) {
            throw new ClientException("Class not found: " + className, e);
        } catch (ClassCastException e) {
            throw new ClientException("Could not cast instance of " + className + " to " + clazz.getName());
        }
    }
}
