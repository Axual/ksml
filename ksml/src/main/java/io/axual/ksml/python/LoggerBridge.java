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

import org.graalvm.polyglot.HostAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerBridge {
    // The below method is called from the Python context, so appears unused in the IDE
    @HostAccess.Export
    public PythonLogger getLogger(String loggerName) {
        return new PythonLogger(LoggerFactory.getLogger(loggerName));
    }

    /**
     * The PythonLogger has less ambiguity by removing the multiple object and exception arguments
     */
    public static class PythonLogger {
        /** The instance's wrapped Slf4J {@link org.slf4j.Logger}. */
        private final Logger logger;

        /**
         * Hidden constructor, construction only happens via {@link #getLogger(String)} in the outer class.
         * @param logger
         */
        private PythonLogger(Logger logger) {
            this.logger = logger;
        }

        @HostAccess.Export
        public String getName() {
            return logger.getName();
        }

        @HostAccess.Export
        public boolean isTraceEnabled() {
            return logger.isTraceEnabled();
        }

        @HostAccess.Export
        public void trace(String s) {
            logger.trace(s);
        }

        @HostAccess.Export
        public void trace(String s, Object o) {
            logger.trace(s, o);
        }

        @HostAccess.Export
        public void trace(String s, Object... objects) {
            logger.trace(s, objects);
        }

        @HostAccess.Export
        public boolean isDebugEnabled() {
            return logger.isDebugEnabled();
        }

        @HostAccess.Export
        public void debug(String s) {
            logger.debug(s);
        }

        @HostAccess.Export
        public void debug(String s, Object o) {
            logger.debug(s, o);
        }

        @HostAccess.Export
        public void debug(String s, Object... objects) {
            logger.debug(s, objects);
        }

        @HostAccess.Export
        public boolean isInfoEnabled() {
            return logger.isInfoEnabled();
        }

        @HostAccess.Export
        public void info(String s) {
            logger.info(s);
        }

        @HostAccess.Export
        public void info(String s, Object o) {
            logger.info(s, o);
        }

        @HostAccess.Export
        public void info(String s, Object... objects) {
            logger.info(s, objects);
        }

        @HostAccess.Export
        public boolean isWarnEnabled() {
            return logger.isWarnEnabled();
        }

        @HostAccess.Export
        public void warn(String s) {
            logger.warn(s);
        }

        @HostAccess.Export
        public void warn(String s, Object o) {
            logger.warn(s, o);
        }

        @HostAccess.Export
        public void warn(String s, Object... objects) {
            logger.warn(s, objects);
        }

        @HostAccess.Export
        public boolean isErrorEnabled() {
            return logger.isErrorEnabled();
        }

        @HostAccess.Export
        public void error(String s) {
            logger.error(s);
        }

        @HostAccess.Export
        public void error(String s, Object o) {
            logger.error(s, o);
        }

        @HostAccess.Export
        public void error(String s, Object... objects) {
            logger.error(s, objects);
        }

    }
}
