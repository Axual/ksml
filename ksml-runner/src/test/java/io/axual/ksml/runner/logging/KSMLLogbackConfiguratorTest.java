package io.axual.ksml.runner.logging;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.net.URL;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        assertNotNull(appenders);
        assertEquals(1, appenders.size());
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference in URL format")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToResourceUrl")
    void configureWithEnvironmentVariableToResourceURL() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertNotNull(resourceUrl);

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> resourceUrl.toExternalForm();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("testEnvToResourceUrl");
        assertNotNull(appenders);
        assertEquals(1, appenders.size());
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference as an absolute filepath")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToFile")
    void configureWithEnvironmentVariableToFile() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertNotNull(resourceUrl);

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> resourceUrl.getPath();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("testEnvToFile");
        assertNotNull(appenders);
        assertEquals(1, appenders.size());
    }

    @Test
    @DisplayName("The configuration file should not be loaded, but fall back to the default setting")
    @SetSystemProperty(key = "logback.test.id", value = "shouldNotAppear")
    void configureWithoutEnvironmentVariable() {
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.environmentVariableLookup = _ -> null;
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        System.out.println(MockAppender.APPENDERS);

        // This id comes from the logback-test.xml, which should be loaded now and is hardcoded
        Collection<MockAppender> appenders = MockAppender.APPENDERS.get("fixed-from-standard-joran-lookup");
        assertNotNull(appenders);
        assertEquals(1, appenders.size());

        // This id is set, but since the default logback-test.xml is used it should never be set
        appenders = MockAppender.APPENDERS.get("shouldNotAppear");
        assertNotNull(appenders);
        assertEquals(0, appenders.size());
    }

}
