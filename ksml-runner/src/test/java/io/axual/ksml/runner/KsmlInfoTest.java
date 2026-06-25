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

import io.axual.ksml.metric.Metrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class KsmlInfoTest {

    /**
     * Builds the same object name {@link KsmlInfo#registerKsmlAppInfo(String)} uses, so the test can
     * look the registered MBean up afterwards.
     */
    private static ObjectName objectNameFor(String appId) throws Exception {
        final var name = "%s:type=app-info,app-id=%s,app-name=%s,app-version=%s,build-time=%s".formatted(
                Metrics.DOMAIN,
                appId,
                ObjectName.quote(KsmlInfo.APP_NAME),
                ObjectName.quote(KsmlInfo.APP_VERSION),
                ObjectName.quote(KsmlInfo.BUILD_TIME));
        return ObjectName.getInstance(name);
    }

    @Test
    @DisplayName("App info is loaded from the ksml-info.properties on the classpath")
    void loadsAppInfoFromProperties() {
        // Values come from src/test/resources/ksml/ksml-info.properties
        assertThat(KsmlInfo.APP_NAME).isEqualTo("ksml-for-testing");
        assertThat(KsmlInfo.APP_VERSION).isEqualTo("testing");
        assertThat(KsmlInfo.BUILD_TIME).isEqualTo("2024-01-01T01:00:00Z");
    }

    @Test
    @DisplayName("registerKsmlAppInfo registers the app-info MBean and is idempotent on repeated calls")
    void registersAppInfoMBeanIdempotently() throws Exception {
        final var appId = "ksmlinfo-test-" + System.nanoTime();
        final var objectName = objectNameFor(appId);
        final var beanServer = ManagementFactory.getPlatformMBeanServer();

        // KsmlInfo.BEAN_CONTENT is a shared static instance, so this test registers a single
        // object name and unregisters it afterwards to keep the platform MBean server clean.
        try {
            assertThat(beanServer.isRegistered(objectName)).isFalse();

            KsmlInfo.registerKsmlAppInfo(appId);

            assertThat(beanServer.isRegistered(objectName)).isTrue();
            assertThat(beanServer.getAttribute(objectName, "Value")).isEqualTo(1);

            // A second call for the same app-id hits the "already registered" branch and must not throw.
            assertThatCode(() -> KsmlInfo.registerKsmlAppInfo(appId)).doesNotThrowAnyException();
            assertThat(beanServer.isRegistered(objectName)).isTrue();
        } finally {
            if (beanServer.isRegistered(objectName)) {
                beanServer.unregisterMBean(objectName);
            }
        }
    }

    @Test
    @DisplayName("The KsmlInfoMBean always reports a value of 1")
    void mBeanReportsConstantValue() {
        assertThat(KsmlInfo.BEAN_CONTENT.getValue()).isEqualTo(1);
        assertThat(new KsmlInfo.KsmlInfoMBean().getValue()).isEqualTo(1);
    }
}
