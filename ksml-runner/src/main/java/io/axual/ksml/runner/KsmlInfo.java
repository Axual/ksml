package io.axual.ksml.runner;

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

import io.axual.ksml.metric.KSMLMetrics;
import lombok.extern.slf4j.Slf4j;

import javax.management.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

@Slf4j
public class KsmlInfo {

    private static final String DEFAULT_APP_NAME = "KSML";
    private static final String DEFAULT_APP_VERSION = "";
    private static final String DEFAULT_BUILD_TIME = "";
    public static final String APP_NAME;
    public static final String APP_VERSION;
    public static final String BUILD_TIME;

    public static final KsmlInfoMBean BEAN_CONTENT = new KsmlInfoMBean();

    static {

        String appName, appVersion, buildTime;
        try {
            ClassLoader cl = KSMLRunner.class.getClassLoader();

            try (InputStream url = cl.getResourceAsStream("ksml/ksml-info.properties")) {
                Properties ksmlInfo = new Properties();
                ksmlInfo.load(url);
                appName = ksmlInfo.getProperty("name", DEFAULT_APP_NAME);
                appVersion = ksmlInfo.getProperty("version", DEFAULT_APP_VERSION);
                buildTime = ksmlInfo.getProperty("buildTime", DEFAULT_BUILD_TIME);
            }

        } catch (IOException e) {
            log.info("Could not load manifest file, using default values");
            appName = DEFAULT_APP_NAME;
            appVersion = DEFAULT_APP_VERSION;
            buildTime = DEFAULT_BUILD_TIME;
        }

        APP_NAME = appName;
        APP_VERSION = appVersion;
        BUILD_TIME = buildTime;
    }

    public static void registerKsmlAppInfo(String appId) {
        var beanName = "%s:type=app-info,app-id=%s,app-name=%s,app-version=%s,build-time=%s".formatted(KSMLMetrics.DOMAIN, appId, ObjectName.quote(APP_NAME), ObjectName.quote(APP_VERSION), ObjectName.quote(BUILD_TIME));
        try {
            var objectName = ObjectName.getInstance(beanName);
            var beanServer = ManagementFactory.getPlatformMBeanServer();
            if (!beanServer.isRegistered(objectName)) {
                log.debug("Registering KsmlInfoMBean for app-id '{}' and object name '{}'", appId, beanName);
                beanServer.registerMBean(BEAN_CONTENT, objectName);
            } else {
                log.warn("KsmlInfoMBean for app-id '{}' and object name '{}' is already registered", appId, beanName);
            }
        } catch (MalformedObjectNameException | NotCompliantMBeanException |
                 InstanceAlreadyExistsException | MBeanRegistrationException e) {
            log.warn("Could not register KsmlInfoMBean for app-id '{}' and object name '{}'", appId, beanName, e);
        }
    }

    public interface IKsmlInfoMXBean {
        int getValue();
    }

    public static class KsmlInfoMBean implements IKsmlInfoMXBean {

        @Override
        public int getValue() {
            return 1;
        }
    }
}
