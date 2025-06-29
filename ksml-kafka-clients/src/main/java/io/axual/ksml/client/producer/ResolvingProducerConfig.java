package io.axual.ksml.client.producer;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
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

import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TransactionalIdPatternResolver;
import io.axual.ksml.client.util.MapUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ResolvingProducerConfig extends ResolvingClientConfig {

    private static final Logger log = LoggerFactory.getLogger(ResolvingProducerConfig.class);

    public ResolvingProducerConfig(Map<String, Object> configs) {
        super(configs);
        downstreamConfigs.remove(TRANSACTIONAL_ID_PATTERN_CONFIG);

        // Apply resolved transactional id to downstream producer, if a transactional id was set
        if (configs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) instanceof String transactionalId) {
            final var transactionalIdPattern = configs.get(TRANSACTIONAL_ID_PATTERN_CONFIG);
            if (transactionalIdPattern != null) {
                final var transactionalIdResolver = new TransactionalIdPatternResolver(transactionalIdPattern.toString(), MapUtil.toStringValues(configs));
                downstreamConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalIdResolver.resolve(transactionalId));
            } else {
                log.warn("No transactional id pattern configured, leaving as is: transactional.id={}", transactionalId);
            }
        }
    }
}
