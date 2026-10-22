package com.simonking.nexus.websocket.config;

import com.simonking.nexus.websocket.cluster.RedisWsBroadcaster;
import com.simonking.nexus.websocket.cluster.WsClusterBus;
import com.simonking.nexus.websocket.cluster.WsClusterNodeIdentity;
import com.simonking.nexus.websocket.cluster.WsClusterNodeRegistry;
import com.simonking.nexus.websocket.core.LocalWsBroadcaster;
import com.simonking.nexus.websocket.core.WsBroadcaster;
import com.simonking.nexus.websocket.registry.WsClientRegistry;
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
 * 同一时刻容器里只有一个 {@link WsBroadcaster}，注入点只写接口，
 * 既不会出现「两个实现都被注入」，也不会出现「业务代码自己 new 了一个」。
 *
 * <p>注意 {@code havingValue} 的严格匹配：{@code nexus.ws.cluster.enabled} 若被配成空串
 * （例如 compose 里引用了未定义变量），两个 Bean 都不会装配，启动直接报「找不到 WsBroadcaster」——
 * 这比静默装配错要好，但也意味着部署脚本必须把开关归一化成 true/false。
 *
 * @author simonking
 */
@Configuration(proxyBeanMethods = false)
public class WsBroadcasterConfig {

    @Bean
    @ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "false", matchIfMissing = true)
    public WsBroadcaster localWsBroadcaster(WsClientRegistry registry,
                                            ObjectMapper objectMapper,
                                            @Qualifier("wsFanoutExecutor") Executor fanoutExecutor) {
        return new LocalWsBroadcaster(registry, objectMapper, fanoutExecutor);
    }

    @Bean
    @ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
    public WsBroadcaster redisWsBroadcaster(WsClientRegistry registry,
                                            ObjectMapper objectMapper,
                                            WsClusterBus clusterBus,
                                            WsClusterNodeIdentity identity,
                                            WsClusterNodeRegistry nodeRegistry,
                                            @Qualifier("wsFanoutExecutor") Executor fanoutExecutor) {
        return new RedisWsBroadcaster(registry, objectMapper, clusterBus, identity, nodeRegistry, fanoutExecutor);
    }
}
