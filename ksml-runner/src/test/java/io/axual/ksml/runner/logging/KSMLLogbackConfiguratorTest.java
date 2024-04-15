package io.axual.ksml.runner.logging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.RestoreEnvironmentVariables;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collection;
import java.util.Map;

import ch.qos.logback.classic.LoggerContext;
import lombok.SneakyThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KSMLLogbackConfiguratorTest {

    LoggerContext spiedContext = new LoggerContext();

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference pointing to a resource")
    @SetEnvironmentVariable(key = "LOGBACK_CONFIGURATION_FILE", value = "logback-custom-testing.xml")
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToResource")
    void configureWithEnvironmentVariableToResource() {
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appender = MockAppender.APPENDERS.get("testEnvToResource");
        assertNotNull(appender);
        assertEquals(1, appender.size());
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference in URL format")
    @RestoreEnvironmentVariables
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToResourceUrl")
    void configureWithEnvironmentVariableToResourceURL() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertNotNull(resourceUrl);
        setEnvVar("LOGBACK_CONFIGURATION_FILE", resourceUrl.toExternalForm());

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appender = MockAppender.APPENDERS.get("testEnvToResourceUrl");
        assertNotNull(appender);
        assertEquals(1, appender.size());
    }

    @Test
    @DisplayName("The configuration file is loaded from an environment variable reference as an absolute filepath")
    @RestoreEnvironmentVariables
    @SetSystemProperty(key = "logback.test.id", value = "testEnvToFile")
    void configureWithEnvironmentVariableToFile() {
        // Get value for environment variable
        URL resourceUrl = getClass().getClassLoader().getResource("logback-custom-testing.xml");
        assertNotNull(resourceUrl);
        setEnvVar("LOGBACK_CONFIGURATION_FILE", resourceUrl.getPath());

        // Run test
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);
        Collection<MockAppender> appender = MockAppender.APPENDERS.get("testEnvToFile");
        assertNotNull(appender);
        assertEquals(1, appender.size());
    }

    @Test
    @DisplayName("The configuration file should not be loaded, but fall back to the default setting")
    @ClearEnvironmentVariable(key = "LOGBACK_CONFIGURATION_FILE")
    @SetSystemProperty(key = "logback.test.id", value = "shouldNotAppear")
    void configureWithoutEnvironmentVariable() {
        KSMLLogbackConfigurator configurator = new KSMLLogbackConfigurator();
        configurator.setContext(spiedContext);
        configurator.configure(spiedContext);

        // This id comes from the logback-test.xml, which should be loaded now and is hardcoded
        Collection<MockAppender> appender = MockAppender.APPENDERS.get("fixed-from-standard-joran-lookup");
        assertNotNull(appender);
        assertEquals(1, appender.size());

        // This id is set, but since the default logback-test.xml is used it should never be set
        appender = MockAppender.APPENDERS.get("shouldNotAppear");
        assertNotNull(appender);
        assertEquals(0, appender.size());
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    void setEnvVar(String key, String value) {
        Class<?> classOfMap = System.getenv().getClass();
        Field field = classOfMap.getDeclaredField("m");
        field.setAccessible(true);
        Map<String, String> writeableEnvironmentVariables = (Map<String, String>) field.get(System.getenv());
        writeableEnvironmentVariables.put(key, value);
    }

}