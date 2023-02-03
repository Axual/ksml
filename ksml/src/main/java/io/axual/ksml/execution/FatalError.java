package io.axual.ksml.execution;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLSchemaException;
import io.axual.ksml.parser.YamlNode;

public class FatalError {
    private static final Logger LOG = LoggerFactory.getLogger(FatalError.class);

    private FatalError() {
    }

    public static RuntimeException configError(String message) {
        return dataError(message, null);
    }

    public static RuntimeException dataError(String message) {
        return dataError(message, null);
    }

    public static RuntimeException dataError(String message, Throwable cause) {
        return reportAndExit(new KSMLDataException(message, cause));
    }

    public static RuntimeException executionError(String message) {
        return executionError(message, null);
    }

    public static RuntimeException executionError(String message, Throwable cause) {
        return reportAndExit(new KSMLExecutionException(message, cause));
    }

    public static RuntimeException parseError(YamlNode node, String message) {
        return parseError(node, message, null);
    }

    public static RuntimeException parseError(YamlNode node, String message, Throwable cause) {
        return reportAndExit(new KSMLParseException(node, message, cause));
    }

    public static RuntimeException schemaError(String message, Throwable cause) {
        return reportAndExit(new KSMLSchemaException(message, cause));
    }

    public static RuntimeException schemaError(String message) {
        return schemaError(message, (Throwable) null);
    }

    public static <T> T schemaError(String message, Class<T> returnType) {
        throw schemaError(message);
    }

    public static RuntimeException reportAndExit(RuntimeException e) {
        LOG.error("\n\n");
        LOG.error("Fatal error");
        printLineSeparator();
        printExceptionDetails(e);
        System.exit(1);
        return e;
    }

    private static void printExceptionDetails(Throwable t) {
        if (t.getCause() != null) {
            printExceptionDetails(t.getCause());
            printLineSeparator();
            LOG.error("Above error caused: {}", t.getMessage());
        } else {
            LOG.error("Description: {}", t.getMessage());
        }
        LOG.error("Stack trace:");
        for (var ste : t.getStackTrace()) {
            LOG.error("  {}::{} @ {}:{}", ste.getClassName(), ste.getMethodName(), ste.getFileName(), ste.getLineNumber());
        }
    }

    private static void printLineSeparator() {
        LOG.error("===========");
    }
}
