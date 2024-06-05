package io.axual.ksml.python;

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
import org.slf4j.event.Level;
import org.slf4j.helpers.CheckReturnValue;
import org.slf4j.spi.LoggingEventBuilder;

public class LoggerBridge {
    // The below method is called from the Python context, so appears unused in the IDE
    public PythonLogger getLogger(String loggerName) {
        return new PythonLogger(LoggerFactory.getLogger(loggerName));
    }

    /**
     * The PythonLogger has less ambiguity by removing the multiple object and exception arguments
     */
    public static class PythonLogger{
        public String getName() {
            return logger.getName();
        }

        @CheckReturnValue
        public LoggingEventBuilder atLevel(Level level) {
            return logger.atLevel(level);
        }

        public boolean isEnabledForLevel(Level level) {
            return logger.isEnabledForLevel(level);
        }

        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        public void trace(String s) {
            logger.trace(s);
        }

        public void trace(String s, Object o) {
            logger.trace(s, o);
        }

        public void trace(String s, Object... objects) {
            logger.trace(s, objects);
        }

        @CheckReturnValue
        public LoggingEventBuilder atTrace() {
            return logger.atTrace();
        }

        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        public void debug(String s) {
            logger.debug(s);
        }

        public void debug(String s, Object o) {
            logger.debug(s, o);
        }

        public void debug(String s, Object... objects) {
            logger.debug(s, objects);
        }

        @CheckReturnValue
        public LoggingEventBuilder atDebug() {
            return logger.atDebug();
        }

        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        public void info(String s) {
            logger.info(s);
        }

        public void info(String s, Object o) {
            logger.info(s, o);
        }

        public void info(String s, Object... objects) {
            logger.info(s, objects);
        }

        @CheckReturnValue
        public LoggingEventBuilder atInfo() {
            return logger.atInfo();
        }

        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        public void warn(String s) {
            logger.warn(s);
        }

        public void warn(String s, Object o) {
            logger.warn(s, o);
        }

        public void warn(String s, Object o, Object o1) {
            logger.warn(s, o, o1);
        }

        @CheckReturnValue
        public LoggingEventBuilder atWarn() {
            return logger.atWarn();
        }

        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        public void error(String s) {
            logger.error(s);
        }

        public void error(String s, Object o) {
            logger.error(s, o);
        }

        public void error(String s, Object... objects) {
            logger.error(s, objects);
        }


        @CheckReturnValue
        public LoggingEventBuilder atError() {
            return logger.atError();
        }

        private final Logger logger;

        private PythonLogger(Logger logger) {
            this.logger = logger;
        }

    }
}
