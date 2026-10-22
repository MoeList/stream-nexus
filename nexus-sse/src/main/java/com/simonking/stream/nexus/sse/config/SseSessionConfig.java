package com.simonking.stream.nexus.sse.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;
import org.springframework.session.data.redis.RedisSessionRepository;

import java.time.Duration;

/**
 * 集群模式的管理页登录态共享：Spring Session + Redis
 *
 * <p>解决什么：登录态原本在容器内存 session 里，请求落到另一个节点就要重新登录；
 * 集群模式把 session 放进 Redis，任一节点登录全集群可用，登出也立刻在所有节点失效。
 *
 * <p>为什么不用 Boot 的自动配置：{@code spring-boot-starter-session-data-redis} 只要在
 * classpath 上，Boot 就会给所有模式装配 Redis 会话，而单机模式（{@code cluster.enabled=false}）
 * 必须不依赖 Redis。因此分两步：
 * <ul>
 *   <li>启动类排除 {@code SessionDataRedisAutoConfiguration}：单机不产生任何 Session Bean，
 *       继续用容器内存 session，行为与改造前一致；</li>
 *   <li>本配置只在集群模式生效，显式声明 {@link RedisSessionRepository}，
 *       由 Boot 的 {@code SessionAutoConfiguration} 负责把过滤器注册到正确的位置。</li>
 * </ul>
 *
 * <p>选非索引仓库（{@link RedisSessionRepository}）：索引仓库靠 keyspace notification
 * 清理过期索引，老版本 Redis / 云代理上经常配不了权限；本服务不需要「按用户查会话」，
 * 一个会话一个 Key 就够。
 *
 * <p>Cookie 名等属性交给 Boot 的 {@code SessionAutoConfiguration}：它按
 * {@code server.servlet.session.cookie.*} 创建 {@code DefaultCookieSerializer}。
 * 这里<b>不要</b>再声明 {@code CookieSerializer} Bean——那会顶掉 Boot 的那份，
 * 导致 same-site / secure / http-only / path 等配置全部失效。
 *
 * @author simonking
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
@EnableSpringHttpSession
public class SseSessionConfig {

    /**
     * 会话命名空间：跟着总线前缀走
     *
     * <p>SSE 与 WebSocket 共用一个 Redis 时必须分开（{@code nexus:sse} / {@code nexus:ws}），
     * 否则两个服务的会话会互相覆盖——Cookie 名不同，但 Redis Key 会撞。
     */
    static String sessionNamespace(ClusterProperties properties) {
        return properties.getKeyPrefix() + ":session";
    }

    /**
     * 会话仓库：一个会话一个 Hash，Key 形如 {@code <key-prefix>:session:sessions:<sessionId>}
     *
     * <p>序列化单独建一个模板而不是复用 Boot 的 {@code RedisTemplate<Object,Object>}：
     * Key 与 Hash field 必须是可读字符串（排障时能直接看），值用 JDK 序列化
     * （会话内容是本服务自己写的登录标记，没有跨语言需求）。
     */
    @Bean
    public RedisSessionRepository sessionRepository(RedisConnectionFactory connectionFactory,
                                                    ClusterProperties properties,
                                                    @Value("${spring.session.timeout:30m}") Duration timeout) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(RedisSerializer.string());
        template.setHashKeySerializer(RedisSerializer.string());
        template.setValueSerializer(RedisSerializer.java());
        template.setHashValueSerializer(RedisSerializer.java());
        template.afterPropertiesSet();

        RedisSessionRepository repository = new RedisSessionRepository(template);
        repository.setRedisKeyNamespace(sessionNamespace(properties));
        repository.setDefaultMaxInactiveInterval(timeout);
        return repository;
    }
}
