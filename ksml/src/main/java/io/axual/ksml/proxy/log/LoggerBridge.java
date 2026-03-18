package io.axual.ksml.proxy.log;

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

import io.axual.ksml.proxy.base.AbstractProxy;
import org.graalvm.polyglot.HostAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerBridge implements AbstractProxy {
    // The below method is called from the Python context, so appears unused in the IDE
    @HostAccess.Export
    public PythonLogger getLogger(String loggerName) {
        return new PythonLogger(LoggerFactory.getLogger(loggerName));
    }

    /**
     * The PythonLogger has less ambiguity by removing the multiple object and exception arguments
     */
    public static class PythonLogger implements AbstractProxy {
        /**
         * The instance's wrapped Slf4J {@link org.slf4j.Logger}.
         */
        private final Logger delegate;

        /**
         * Hidden constructor, construction only happens via {@link #getLogger(String)} in the outer class.
         *
         * @param delegate The backing Java logger to use
         */
        private PythonLogger(Logger delegate) {
            this.delegate = delegate;
        }

        @HostAccess.Export
        public String getName() {
            return delegate.getName();
        }

        @HostAccess.Export
        public boolean isTraceEnabled() {
            return delegate.isTraceEnabled();
        }

        @HostAccess.Export
        public void trace(String s) {
            delegate.trace(s);
        }

        @HostAccess.Export
        public void trace(String s, Object o) {
            delegate.trace(s, o);
        }

        @HostAccess.Export
        public void trace(String s, Object... objects) {
            delegate.trace(s, objects);
        }

        @HostAccess.Export
        public boolean isDebugEnabled() {
            return delegate.isDebugEnabled();
        }

        @HostAccess.Export
        public void debug(String s) {
            delegate.debug(s);
        }

        @HostAccess.Export
        public void debug(String s, Object o) {
            delegate.debug(s, o);
        }

        @HostAccess.Export
        public void debug(String s, Object... objects) {
            delegate.debug(s, objects);
        }

        @HostAccess.Export
        public boolean isInfoEnabled() {
            return delegate.isInfoEnabled();
        }

        @HostAccess.Export
        public void info(String s) {
            delegate.info(s);
        }

        @HostAccess.Export
        public void info(String s, Object o) {
            delegate.info(s, o);
        }

        @HostAccess.Export
        public void info(String s, Object... objects) {
            delegate.info(s, objects);
        }

        @HostAccess.Export
        public boolean isWarnEnabled() {
            return delegate.isWarnEnabled();
        }

        @HostAccess.Export
        public void warn(String s) {
            delegate.warn(s);
        }

        @HostAccess.Export
        public void warn(String s, Object o) {
            delegate.warn(s, o);
        }

        @HostAccess.Export
        public void warn(String s, Object... objects) {
            delegate.warn(s, objects);
        }

        @HostAccess.Export
        public boolean isErrorEnabled() {
            return delegate.isErrorEnabled();
        }

        @HostAccess.Export
        public void error(String s) {
            delegate.error(s);
        }

        @HostAccess.Export
        public void error(String s, Object o) {
            delegate.error(s, o);
        }

        @HostAccess.Export
        public void error(String s, Object... objects) {
            delegate.error(s, objects);
        }
    }
}
