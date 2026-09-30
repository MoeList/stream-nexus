package com.simonking.stream.nexus.sse.config;

import com.simonking.stream.nexus.sse.cluster.ClusterBus;
import com.simonking.stream.nexus.sse.cluster.ClusterNodeIdentity;
import com.simonking.stream.nexus.sse.cluster.ClusterNodeRegistry;
import com.simonking.stream.nexus.sse.cluster.RedisSseBroadcaster;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import com.simonking.stream.nexus.sse.core.LocalSseBroadcaster;
import com.simonking.stream.nexus.sse.core.SseBroadcaster;
import com.simonking.stream.nexus.sse.core.SseSender;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Executor;

/**
 * 广播策略装配
 *
 * <p>单机与集群两个实现用 {@code @ConditionalOnProperty} 严格二选一：
 * 同一时刻容器里只有一个 {@link SseBroadcaster}，注入点只写接口，
 * 既不会出现「两个实现都被注入」，也不会出现「业务代码自己 new 了一个」。
 *
 * @author simonking
 */
@Configuration
public class SseBroadcasterConfig {

    @Bean
    @ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "false", matchIfMissing = true)
    public SseBroadcaster localSseBroadcaster(SseClientRegistry registry,
                                              SseSender sender,
                                              ObjectMapper objectMapper,
                                              @Qualifier("sseFanoutExecutor") Executor fanoutExecutor) {
        return new LocalSseBroadcaster(registry, sender, objectMapper, fanoutExecutor);
    }

    @Bean
    @ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
    public SseBroadcaster redisSseBroadcaster(SseClientRegistry registry,
                                              SseSender sender,
                                              ObjectMapper objectMapper,
                                              ClusterBus clusterBus,
                                              ClusterNodeIdentity identity,
                                              ClusterNodeRegistry nodeRegistry,
                                              @Qualifier("sseFanoutExecutor") Executor fanoutExecutor) {
        return new RedisSseBroadcaster(registry, sender, objectMapper, clusterBus, identity, nodeRegistry, fanoutExecutor);
    }
}
