package io.axual.ksml.runner.config;

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

import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig;
import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig.Handler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorHandlingConfigTest {

    @Test
    @DisplayName("Each accessor lazily creates a default config with its own logger name")
    void accessorsCreateDefaultsWithChannelLoggerNames() {
        final var config = new ErrorHandlingConfig(); // consume/produce/process all null

        final var consume = config.consumerErrorHandlingConfig();
        final var produce = config.producerErrorHandlingConfig();
        final var process = config.processErrorHandlingConfig();

        assertThat(consume.loggerName()).isEqualTo("ConsumeError");
        assertThat(produce.loggerName()).isEqualTo("ProduceError");
        assertThat(process.loggerName()).isEqualTo("ProcessError");

        // The lazily created defaults use the documented defaults: logging on, stop-on-fail.
        assertThat(consume.log()).isTrue();
        assertThat(consume.logPayload()).isFalse();
        assertThat(consume.handler()).isEqualTo(Handler.STOP);
    }

    @Test
    @DisplayName("Accessors are idempotent and keep returning the same instance")
    void accessorsAreIdempotent() {
        final var config = new ErrorHandlingConfig();

        final var first = config.consumerErrorHandlingConfig();
        final var second = config.consumerErrorHandlingConfig();

        assertThat(second).isSameAs(first);
    }

    @Test
    @DisplayName("An explicitly configured logger name is preserved")
    void preservesExplicitLoggerName() {
        final var custom = new ErrorTypeHandlingConfig();
        custom.loggerName("MyConsumeLogger");
        custom.handler(Handler.CONTINUE);
        final var config = new ErrorHandlingConfig();
        config.consume(custom);

        final var consume = config.consumerErrorHandlingConfig();

        assertThat(consume).isSameAs(custom);
        assertThat(consume.loggerName()).isEqualTo("MyConsumeLogger");
        assertThat(consume.handler()).isEqualTo(Handler.CONTINUE);
    }

    @Test
    @DisplayName("A preset config without a logger name gets the channel default filled in for every channel")
    void fillsMissingLoggerNameOnPresetConfig() {
        final var presetConsume = new ErrorTypeHandlingConfig(); // loggerName is null
        final var presetProduce = new ErrorTypeHandlingConfig();
        final var presetProcess = new ErrorTypeHandlingConfig();
        final var config = new ErrorHandlingConfig();
        config.consume(presetConsume);
        config.produce(presetProduce);
        config.process(presetProcess);

        // The same instances are kept, but their missing logger names are defaulted per channel.
        assertThat(config.consumerErrorHandlingConfig()).isSameAs(presetConsume);
        assertThat(presetConsume.loggerName()).isEqualTo("ConsumeError");
        assertThat(config.producerErrorHandlingConfig()).isSameAs(presetProduce);
        assertThat(presetProduce.loggerName()).isEqualTo("ProduceError");
        assertThat(config.processErrorHandlingConfig()).isSameAs(presetProcess);
        assertThat(presetProcess.loggerName()).isEqualTo("ProcessError");
    }
}
