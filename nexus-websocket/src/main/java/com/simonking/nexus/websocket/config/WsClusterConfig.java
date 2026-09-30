package com.simonking.nexus.websocket.config;

import com.simonking.nexus.websocket.cluster.WsClusterMessageListener;
import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import io.lettuce.core.protocol.ProtocolVersion;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientOptionsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 集群订阅装配
 *
 * <p>整个进程只有一个监听容器、一个 listener，按频道注册两次：
 * 广播推送 + 控制指令。不要为每个频道单独建容器，也不要手动 start——
 * 容器实现 {@code SmartLifecycle}，交给 Spring 管理启停。
 *
 * <p>比 SSE 少一个点对点频道：WebSocket 没有跨节点 PONG 转发，
 * 自然不需要 {@code node:{nodeId}} 订阅。
 *
 * <p><b>依赖必须全部放在 {@code @Bean} 方法参数上</b>，不能在本类构造器里注入 listener：
 * 容器创建要 {@code RedisConnectionFactory}，而 listener 的依赖链最终又回到 RedisTemplate，
 * 构造器注入会让「配置类实例」成为环上的一环，集群模式下直接启动失败。
 *
 * @author simonking
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
public class WsClusterConfig {

    @Bean
    public RedisMessageListenerContainer wsClusterListenerContainer(RedisConnectionFactory connectionFactory,
                                                                   WsClusterMessageListener listener,
                                                                   WsClusterProperties properties,
                                                                   @Qualifier("wsRedisListenerExecutor") ThreadPoolTaskExecutor messageExecutor,
                                                                   @Qualifier("wsRedisSubscriptionExecutor") ThreadPoolTaskExecutor subscriptionExecutor) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        // 默认的 SimpleAsyncTaskExecutor 每条消息新建一个线程，高频推送下线程创建/销毁本身就是瓶颈
        container.setTaskExecutor(messageExecutor);
        // 订阅握手是阻塞操作，单独一个池，避免把消息分发一起卡住
        container.setSubscriptionExecutor(subscriptionExecutor);
        container.addMessageListener(listener, new ChannelTopic(ClusterConstants.channelPush(properties.getKeyPrefix())));
        container.addMessageListener(listener, new ChannelTopic(ClusterConstants.channelControl(properties.getKeyPrefix())));
        return container;
    }

    /**
     * RESP2 兼容开关
     *
     * <p>Lettuce 默认按 RESP3 握手（需要 Redis 6+）。老版本 Redis、部分云厂商代理
     * 以及只实现 RESP2 的测试替身对不上订阅确认的响应类型，表现为启动期
     * {@code Subscription registration timeout exceeded}。
     * 遇到这类服务端时把 {@code nexus.ws.cluster.redis-protocol} 设为 {@code resp2}。
     */
    @Bean
    @ConditionalOnProperty(name = "nexus.ws.cluster.redis-protocol", havingValue = "resp2")
    public LettuceClientOptionsBuilderCustomizer resp2ClientOptionsCustomizer() {
        return builder -> builder.protocolVersion(ProtocolVersion.RESP2);
    }
}
