package io.axual.ksml.client.admin;

import io.axual.ksml.client.generic.ResolvingClientConfig;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

public class ResolvingAdmin extends ProxyAdmin {
    private final ResolvingClientConfig config;

    public ResolvingAdmin(Map<String, Object> configs) {
        config = new ResolvingClientConfig(configs);
        initializeAdmin(AdminClient.create(config.getDownstreamConfigs()));
    }
}
