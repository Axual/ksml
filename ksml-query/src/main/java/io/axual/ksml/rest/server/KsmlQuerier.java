package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;

import java.util.Collection;

/**
 * Class to transfer data from the KSML components to the REST services.
 */
public interface KsmlQuerier {
    /**
     * Get all the Streams Metadata for a specific store
     *
     * @param storeName the name of the store
     * @return a collection of Streams Metadata
     */
    Collection<StreamsMetadata> allMetadataForStore(String storeName);

    /**
     * Get the metadata for a store and a specific key. For example which instance is assigned to the partition for the key
     *
     * @param storeName     the name of the store containing the key
     * @param key           the specific key for which the metadata is needed
     * @param keySerializer the serializer needed to convert the key to bytes
     * @param <K>           The type of the key
     * @return the Metadata for the specified store and key
     */
    <K> KeyQueryMetadata queryMetadataForKey(String storeName, K key, Serializer<K> keySerializer);


    /**
     * Get a specific store using the provided
     *
     * @param storeQueryParameters the parameters to find the correct store
     * @param <T>                  the type of Store to return
     * @return the Store found
     * @throws org.apache.kafka.streams.errors.UnknownStateStoreException if the store does not exist
     */
    <T> T store(StoreQueryParameters<T> storeQueryParameters);

    /**
     * Get the {@link ComponentState} for the Kafka Streams component of KSML
     *
     * @return the {@link ComponentState} for the Kafka Streams component, and {@link ComponentState#NOT_APPLICABLE} if the component isn't used
     */
    ComponentState getStreamRunnerState();

    /**
     * Get the {@link ComponentState} for the Producer component of KSML
     *
     * @return the {@link ComponentState} for the Producer component, and {@link ComponentState#NOT_APPLICABLE} if the component isn't used
     */
    ComponentState getProducerState();
}
