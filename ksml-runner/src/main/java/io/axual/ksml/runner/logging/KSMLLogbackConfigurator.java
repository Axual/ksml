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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Set;

import ch.qos.logback.classic.ClassicConstants;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ConfiguratorRank;
import ch.qos.logback.classic.util.DefaultJoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.status.InfoStatus;
import ch.qos.logback.core.status.StatusManager;
import ch.qos.logback.core.status.WarnStatus;
import ch.qos.logback.core.util.Loader;
import ch.qos.logback.core.util.OptionHelper;

/**
 * This configurator class is used to support determining and loading the logback configuration file using
 * a path set in an environment variable.
 * <br/>
 * Priority of configuration is:
 * <ol>
 *     <li>System property <i>logback.configurationFile</i></li>
 *     <li>Environment variable <i>LOGBACK_CONFIGURATION_FILE</i></li>
 *     <li>Classpath file <i>logback-test.xml</i></li>
 *     <li>Classpath file <i>logback.xml</i></li>
 * </ol>
 *
 * The configurator will not proceed to the next steps if the system property or environment variable is set to an incorrect value.
 */
@ConfiguratorRank(value = ConfiguratorRank.CUSTOM_HIGH_PRIORITY)
public class KSMLLogbackConfigurator extends DefaultJoranConfigurator {
    public static final String CONFIG_FILE_ENV_PROPERTY = "LOGBACK_CONFIGURATION_FILE";
    public static final String CONFIG_FILE_SYS_PROPERTY = ClassicConstants.CONFIG_FILE_PROPERTY;

    @Override
    public ExecutionStatus configure(LoggerContext context) {
        ClassLoader classLoader = Loader.getClassLoaderOfObject(this);
        // System properties should take precedence
        URL url = findConfigFileURLFromSystemProperties(classLoader);
        if (url == null) {
            // Get the configuration from environment variables, if set
            url = findConfigFileURLFromEnvironmentVariables(classLoader);
        }
        if (url != null) {
            try {
                System.err.printf("Using URL to config %s%n", url);
                configureByResource(url);
            } catch (JoranException e) {
                context.getStatusManager().add(new WarnStatus("Could not configure KSML logging", this, e));
            }

            return ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY;
        }

        // No environment variable URL found, use DefaultJoran logic
        return super.configure(context);
    }

    private URL findConfigFileURLFromEnvironmentVariables(ClassLoader classLoader) {
        return findConfigFileURL(classLoader, OptionHelper.getEnv(CONFIG_FILE_ENV_PROPERTY));
    }

    private URL findConfigFileURLFromSystemProperties(ClassLoader classLoader) {
        return findConfigFileURL(classLoader, OptionHelper.getSystemProperty(CONFIG_FILE_SYS_PROPERTY));
    }

    private URL findConfigFileURL(ClassLoader classLoader, String logbackConfigFile) {
        if (logbackConfigFile != null && !logbackConfigFile.isBlank()) {
            URL url = null;
            try {
                url = new URI(logbackConfigFile.trim()).toURL();
                return url;
            } catch (URISyntaxException | MalformedURLException | IllegalArgumentException e) {
                // so, resource is not a URL:
                // attempt to get the resource from the class path
                url = Loader.getResource(logbackConfigFile, classLoader);
                if (url != null) {
                    return url;
                }
                // OK, check if the config is a file?
                File f = new File(logbackConfigFile);
                if (f.exists() && f.isFile()) {
                    try {
                        url = f.toURI().toURL();
                        return url;
                    } catch (MalformedURLException e1) {
                        // Eat exception
                    }
                }
            } finally {
                StatusManager sm = context.getStatusManager();
                if (url == null) {
                    // Information, could not find the resource
                    sm.add(new InfoStatus("Could NOT find resource [" + logbackConfigFile + "]", context));
                } else {
                    // OK, a resource url was found
                    sm.add(new InfoStatus("Found resource [" + logbackConfigFile + "] at [" + url + "]", context));
                    Set<URL> urlSet = null;
                    try {
                        // Get all resources with the name
                        urlSet = Loader.getResources(logbackConfigFile, classLoader);
                    } catch (IOException e) {
                        // Error on getting the resources
                        addError("Failed to get url list for resource [" + logbackConfigFile + "]", e);
                    }
                    if (urlSet != null && urlSet.size() > 1) {
                        // Multiple resources found, raise general warning and for each of the resources
                        addWarn("Resource [" + logbackConfigFile + "] occurs multiple times on the classpath.");
                        for (URL urlFromSet : urlSet) {
                            addWarn("Resource [" + logbackConfigFile + "] occurs at [" + urlFromSet.toString() + "]");
                        }
                    }
                }
            }
        }
        return null;
    }
}
