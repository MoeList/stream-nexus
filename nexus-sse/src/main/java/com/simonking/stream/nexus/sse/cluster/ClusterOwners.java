package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;

/**
 * 连接归属索引：{@code clientId -> nodeId}
 *
 * <p>PONG 可能落到集群里的任意节点（负载均衡不保证同一客户端粘在同一个节点），
 * 本节点没有该连接时，查这张表把 PONG 定向转发给真正持有连接的节点。
 *
 * <p>读写失败一律降级：Redis 抖动不能阻断建连、也不能让回收线程抛异常。
 * 最坏情况是 PONG 转发不出去，客户端会在下一次心跳继续上报。
 *
 * <p>{@link #bind}/{@link #unbind} 异步执行：建连与回收都在请求/心跳线程上，
 * Redis 超时 3s 时同步写会把整个线程一起拖住。{@link #ownerOf} 保持同步——
 * PONG 转发需要立即拿到结果，且它的调用频率远低于建连。
 *
 * <p>异步化后的顺序风险：同一 clientId 的 bind 与 unbind 可能乱序执行。
 * clientId 是服务端生成的 UUID，重连必然换新值，因此这里不额外做串行化。
 *
 * @author simonking
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
public class ClusterOwners {

    private final StringRedisTemplate redis;

    private final ClusterProperties properties;

    private final ClusterNodeIdentity identity;

    private final Executor ownerExecutor;

    public ClusterOwners(StringRedisTemplate redis, ClusterProperties properties,
                         ClusterNodeIdentity identity,
                         @Qualifier("sseOwnerExecutor") Executor ownerExecutor) {
        this.redis = redis;
        this.properties = properties;
        this.identity = identity;
        this.ownerExecutor = ownerExecutor;
    }

    /**
     * 建连成功后登记归属
     */
    public void bind(String clientId) {
        ownerExecutor.execute(() -> {
            try {
                redis.opsForHash().put(key(), clientId, identity.getNodeId());
            } catch (Exception e) {
                log.warn("[sse] 写入连接归属失败，clientId={}：{}", clientId, e.getMessage());
            }
        });
    }

    /**
     * 连接回收后删除归属；重复删除无害
     */
    public void unbind(String clientId) {
        ownerExecutor.execute(() -> {
            try {
                redis.opsForHash().delete(key(), clientId);
            } catch (Exception e) {
                log.warn("[sse] 删除连接归属失败，clientId={}：{}", clientId, e.getMessage());
            }
        });
    }

    /**
     * 查询连接归属节点；查不到或 Redis 异常返回 null
     */
    public String ownerOf(String clientId) {
        try {
            Object owner = redis.opsForHash().get(key(), clientId);
            return owner == null ? null : owner.toString();
        } catch (Exception e) {
            log.warn("[sse] 查询连接归属失败，clientId={}：{}", clientId, e.getMessage());
            return null;
        }
    }

    private String key() {
        return ClusterConstants.keyOwners(properties.getKeyPrefix());
    }
}
