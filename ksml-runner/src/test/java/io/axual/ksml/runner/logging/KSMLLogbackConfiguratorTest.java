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

import ch.qos.logback.classic.LoggerContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class KSMLLogbackConfiguratorTest {

    LoggerContext spiedContext = new LoggerContext();

    @BeforeEach
    void setUp() {
        // Reset the map and leave it clear for the upcoming tests. Some settings during tests might leave old instances
        MockAppender.APPENDERS.clear();
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference pointing to a resource")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToResource")
    void configureWithEnvironmentVariableToResource() {
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> "logback-custom-testing.xml";
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("testEnvToResource");
        assertThat(appenders).hasSize(1);
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference in URL format")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToResourceUrl")
    void configureWithEnvironmentVariableToResourceURL() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertThat(resourceUrl).isNotNull();

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> resourceUrl.toExternalForm();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("testEnvToResourceUrl");
        assertThat(appenders).hasSize(1);
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference as an absolute filepath")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToFile")
    void configureWithEnvironmentVariableToFile() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertThat(resourceUrl).isNotNull();

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> resourceUrl.getPath();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("testEnvToFile");
        assertThat(appenders).hasSize(1);
    }

    @Test
    @DisplayName("The configuration file should not be loaded, but fall back to the default setting")
    @SetSystemProperty(key = "logback.test.id", value = "shouldNotAppear")
    @SuppressWarnings("java:S106")
    void configureWithoutEnvironmentVariable() {
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> null;
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        System.out.println(MockAppender.APPENDERS);

        // This id comes from the logback-test.xml, which should be loaded now and is hardcoded
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("fixed-from-standard-joran-lookup");
        assertThat(appenders).hasSize(1);

        // This id is set, but since the default logback-test.xml is used it should never be set
        appenders = MockAppender.APPENDERS.get("shouldNotAppear");
        assertThat(appenders).isEmpty();
    }

    @Test
    @DisplayName("A blank environment variable value falls back to the default configuration")
    @SetSystemProperty(key = "logback.test.id", value = "blankShouldNotAppear")
    void configureWithBlankEnvironmentVariable() {
        final var configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> "   "; // blank value is ignored
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);

        // Falls back to the default logback-test.xml.
        assertThat(MockAppender.APPENDERS.get("fixed-from-standard-joran-lookup")).hasSize(1);
        assertThat(MockAppender.APPENDERS.get("blankShouldNotAppear")).isEmpty();
    }

    @Test
    @DisplayName("A non-existent config reference that is neither URL, resource nor file falls back to the default")
    @SetSystemProperty(key = "logback.test.id", value = "missingShouldNotAppear")
    void configureWithUnresolvableReference() {
        final var configurator = new KSMLLogbackConfigurator();
        // Not a valid URL, not a classpath resource and not an existing file -> nothing is resolved.
        configurator.environmentVariableLookup = _ -> "/does/not/exist/logback-missing.xml";
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);

        assertThat(MockAppender.APPENDERS.get("fixed-from-standard-joran-lookup")).hasSize(1);
        assertThat(MockAppender.APPENDERS.get("missingShouldNotAppear")).isEmpty();
    }

    @Test
    @DisplayName("A malformed configuration file is reported as a warning rather than thrown")
    @SetSystemProperty(key = "logback.test.id", value = "malformed")
    @SneakyThrows
    void configureWithMalformedConfigFile(@TempDir Path tempDir) {
        final var malformed = tempDir.resolve("broken-logback.xml");
        Files.writeString(malformed, "<configuration><appender"); // not well-formed XML

        final var configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> malformed.toUri().toString();
        configurator.setContext(spiedContext);
        // configureByResource throws a JoranException, which configure() catches and records as a warning.
        configurator.configure(spiedContext);

        final var hasWarning = spiedContext.getStatusManager().getCopyOfStatusList().stream()
                .anyMatch(status -> status.getMessage().contains("Could not configure KSML logging"));
        assertThat(hasWarning).as("a warning status should be recorded for the malformed config").isTrue();
    }

}
