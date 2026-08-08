package io.axual.ksml.runner.logging;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.Appender;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards that the shipped logback.xml really attaches appenders to the root logger.
 *
 * <p>This exists because of a silent failure that reached a release branch. The logback 1.5.34 to
 * 1.5.37 upgrade in PR #667 stopped an {@code <include>} inside an {@code <if>} branch from registering
 * its appenders. logback reported no error, the root logger ended up with nothing attached, and KSML
 * printed no application logs at all while still processing messages normally. Nothing in the suite
 * noticed, because no test looked at the result of configuration.</p>
 *
 * <p>These tests configure a fresh context from the real logback.xml and assert the outcome, so any
 * future change that leaves the root logger empty fails here instead of in a running deployment.</p>
 */
class LogbackAppenderWiringTest {

    @Test
    @DisplayName("The default (plain text) configuration attaches appenders to the root logger")
    @ClearSystemProperty(key = KSMLLogbackConfigurator.STYLE_FILE_PROPERTY)
    void defaultStyleAttachesAppenders() {
        assertThat(appendersAfterConfiguring())
                .as("root logger appenders; empty means every log line is silently dropped")
                .isNotEmpty();
    }

    @Test
    @DisplayName("The JSON configuration attaches appenders to the root logger")
    @SetSystemProperty(key = KSMLLogbackConfigurator.STYLE_FILE_PROPERTY, value = KSMLLogbackConfigurator.JSON_STYLE_FILE)
    void jsonStyleAttachesAppenders() {
        assertThat(appendersAfterConfiguring())
                .as("root logger appenders for the JSON style")
                .isNotEmpty();
    }

    @Test
    @DisplayName("LOGBACK_USE_JSON is read from the environment")
    @ClearSystemProperty(key = KSMLLogbackConfigurator.USE_JSON_PROPERTY)
    void useJsonLoggingReadsTheEnvironment() {
        final var configurator = new KSMLLogbackConfigurator();

        configurator.environmentVariableLookup = name -> KSMLLogbackConfigurator.USE_JSON_PROPERTY.equals(name) ? "true" : null;
        assertThat(configurator.useJsonLogging()).isTrue();

        configurator.environmentVariableLookup = name -> null;
        assertThat(configurator.useJsonLogging()).isFalse();
    }

    @Test
    @DisplayName("A system property takes precedence over the environment")
    @SetSystemProperty(key = KSMLLogbackConfigurator.USE_JSON_PROPERTY, value = "false")
    void systemPropertyWinsOverEnvironment() {
        final var configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = name -> "true";

        assertThat(configurator.useJsonLogging()).isFalse();
    }

    /** Configures a throwaway context from the packaged logback.xml and returns the root logger's appenders. */
    @SneakyThrows
    private static List<Appender<?>> appendersAfterConfiguring() {
        final var resource = LogbackAppenderWiringTest.class.getClassLoader().getResource("logback.xml");
        assertThat(resource).as("packaged logback.xml must be on the classpath").isNotNull();

        final var context = new LoggerContext();
        try {
            final var configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.doConfigure(resource);

            final var appenders = new ArrayList<Appender<?>>();
            context.getLogger(Logger.ROOT_LOGGER_NAME).iteratorForAppenders().forEachRemaining(appenders::add);
            return appenders;
        } finally {
            context.stop();
        }
    }
}
