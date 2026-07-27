package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
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

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RestServerExceptionMapperTest {

    private final RestServerExceptionMapper mapper = new RestServerExceptionMapper();

    @Test
    @DisplayName("A WebApplicationException is mapped to its own response")
    void usesWebApplicationExceptionResponse() {
        final var response = mapper.toResponse(new NotFoundException("missing"));

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    @DisplayName("A generic throwable is mapped to a 500 plain-text response carrying its message")
    void mapsGenericThrowableTo500() {
        final var response = mapper.toResponse(new IllegalStateException("boom"));

        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(response.getMediaType()).isEqualTo(MediaType.TEXT_PLAIN_TYPE);
        assertThat(response.getEntity()).isEqualTo("500: boom");
    }

    @Test
    @DisplayName("A throwable without a message still maps to a 500 plain-text response")
    void mapsThrowableWithoutMessage() {
        final var response = mapper.toResponse(new IllegalStateException());

        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(response.getEntity()).isEqualTo("500: null");
    }
}
