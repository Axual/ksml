package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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


import io.axual.ksml.type.UserType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.streams.Topology;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class TopicDefinition extends AbstractDefinition {
    private final String topic;
    private final UserType keyType;
    private final UserType valueType;
    private final FunctionDefinition tsExtractor;
    private final Topology.AutoOffsetReset resetPolicy;

    @Override
    public String toString() {
        final var kt = keyType != null ? ", " + keyType : "";
        final var vt = valueType != null ? ", " + valueType : "";
        final var te = tsExtractor != null ? ", " + tsExtractor : "";
        final var rp = resetPolicy != null ? ", " + resetPolicy : "";
        return definitionType() + "[topic=" + topic + kt + vt + te + rp + "]";
    }
}
