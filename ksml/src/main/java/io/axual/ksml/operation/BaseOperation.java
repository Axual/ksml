package io.axual.ksml.operation;

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


import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.parser.StreamOperation;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseOperation implements StreamOperation {
    private static class NameValidator extends Named {
        // Satisfy compiler with dummy constructor
        private NameValidator() {
            super("nonsense");
        }

        // Define a static method that calls the protected validate method in Named
        public static String validateNameAndReturnError(String name) {
            try {
                Named.validate(name);
                return null;
            } catch (TopologyException e) {
                return e.getMessage();
            }
        }
    }

    protected final String name;

    public BaseOperation(String name) {
        var error = NameValidator.validateNameAndReturnError(name);
        if (error != null) {
            log.warn("Ignoring name with error '" + name + "': " + error);
            this.name = null;
        } else {
            this.name = name;
        }
    }

    @Override
    public String toString() {
        var operation = getClass().getSimpleName();
        if (operation.toLowerCase().endsWith("operation")) {
            operation = operation.substring(0, operation.length() - 9);
        }
        return (name == null ? "Unnamed" : name) + " operation " + operation;
    }
}
