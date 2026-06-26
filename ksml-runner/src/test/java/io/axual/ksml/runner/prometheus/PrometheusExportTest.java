package io.axual.ksml.runner.prometheus;

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

import io.axual.ksml.runner.config.PrometheusConfig;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class PrometheusExportTest {

    private static HTTPServer httpServerOf(PrometheusExport export) throws Exception {
        final Field field = PrometheusExport.class.getDeclaredField("httpServer");
        field.setAccessible(true);
        return (HTTPServer) field.get(export);
    }

    @Test
    @DisplayName("When disabled, start() does not open an HTTP server")
    void disabledDoesNotStartServer() throws Exception {
        final var config = new PrometheusConfig(); // enabled defaults to false
        try (final var export = new PrometheusExport(config)) {
            assertThatCode(export::start).doesNotThrowAnyException();
            assertThat(httpServerOf(export)).as("no server when disabled").isNull();
        }
    }

    @Test
    @DisplayName("stop() and close() are safe to call when the server was never started")
    void stopIsIdempotentWhenNeverStarted() {
        final var export = new PrometheusExport(new PrometheusConfig());
        assertThatCode(export::stop).doesNotThrowAnyException();
        assertThatCode(export::close).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("When enabled, start() exposes a working metrics endpoint that close() shuts down")
    void enabledStartsAndServesMetrics() throws Exception {
        final var config = new PrometheusConfig();
        config.enabled(true);
        config.port(0); // bind to an ephemeral port so the test never clashes with a fixed port

        final var export = new PrometheusExport(config);
        try (export; final var httpClient = HttpClient.newHttpClient()) {
            export.start();

            final var server = httpServerOf(export);
            assertThat(server).as("an HTTP server is created when enabled").isNotNull();
            final int boundPort = server.getPort();
            assertThat(boundPort).isPositive();

            // The exporter actually serves Prometheus metrics over HTTP.
            final var response = httpClient.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + boundPort + "/metrics")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isNotEmpty();
        }

        // close() clears the server reference, and a second close() is a harmless no-op.
        assertThat(httpServerOf(export)).isNull();
        assertThatCode(export::close).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("The configuration is defensively copied so later mutations have no effect")
    void configurationIsDefensivelyCopied() throws Exception {
        final var config = new PrometheusConfig();
        config.enabled(false);

        try (final var export = new PrometheusExport(config)) {
            // Enabling the original config after construction must not enable the export.
            config.enabled(true);
            export.start();
            assertThat(httpServerOf(export)).as("export keeps its own disabled copy").isNull();
        }
    }
}
