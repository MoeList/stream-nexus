package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.sse.auth.PushAppRegistry;
import com.simonking.stream.nexus.sse.connection.SseClient;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import com.simonking.stream.nexus.sse.core.SseBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 集群消息分发
 *
 * <p>PUSH 只在本节点扇出，且跳过生产该消息的入口节点（入口节点已经本地扇出过）；
 * PONG 只更新本地连接的心跳时间；KICK / KICK_ALL 走统一回收入口，
 * 这样模块索引、owners 索引、emitter 会一起清理。
 *
 * <p>APPS_CHANGED 让台账变更「立即生效」：写节点更新完自己后广播一次，
 * 其他节点收到就重载，不必等 {@code app-cache-ttl}（默认 30s）到期。
 * 这是缩短窗口的优化，不是正确性的唯一保证——TTL 到期后照样会对齐。
 *
 * <p>SNAPSHOT_REQUEST 由 {@link ClusterSnapshotCollector} 处理：把本节点连接写成一批，
 * 供管理节点聚合。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
public class SseClusterDispatcher {

    private final SseBroadcaster broadcaster;

    private final SseClientRegistry registry;

    private final ClusterNodeIdentity identity;

    private final PushAppRegistry appRegistry;

    private final ClusterSnapshotCollector snapshotCollector;

    public void dispatch(ClusterMessage message) {
        switch (message.getType()) {
            case PUSH -> onPush(message);
            case PONG -> onPong(message);
            case KICK -> onKick(message);
            case KICK_ALL -> onKickAll(message);
            case APPS_CHANGED -> onAppsChanged(message);
            case SNAPSHOT_REQUEST -> onSnapshotRequest(message);
            default -> log.warn("[sse] 未知集群消息类型：{}", message.getType());
        }
    }

    private void onPush(ClusterMessage message) {
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            // 本节点就是入口节点，发布前已经本地扇出过；再发一次就是重复推送
            return;
        }
        broadcaster.pushLocal(message);
    }

    private void onPong(ClusterMessage message) {
        SseClient client = registry.get(message.getClientId());
        if (client == null) {
            log.debug("[sse] 收到转发 PONG 但本节点无此连接，clientId={}", message.getClientId());
            return;
        }
        client.setLastPongTime(System.currentTimeMillis());
        log.debug("[sse] 已更新转发 PONG 的心跳时间，clientId={}, from={}",
                message.getClientId(), message.getOriginNodeId());
    }

    private void onKick(ClusterMessage message) {
        log.debug("[sse] 收到集群踢下线指令，clientId={}, from={}", message.getClientId(), message.getOriginNodeId());
        registry.remove(message.getClientId());
    }

    private void onKickAll(ClusterMessage message) {
        int removed = registry.removeAll();
        if (removed > 0) {
            log.info("[sse] 集群指令强制下线本节点全部连接：{} 条", removed);
        }
    }

    /**
     * 台账变更：本节点写入时内存已同步更新，可以直接跳过
     *
     * <p>其他节点无条件重载，不先比版本号：写入是「HSET + INCR」两步，
     * 两个写者交错时版本号可能相同而内容已不同（详见 {@link PushAppRegistry#reload()}）。
     */
    private void onAppsChanged(ClusterMessage message) {
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            return;
        }
        appRegistry.reload();
    }

    private void onSnapshotRequest(ClusterMessage message) {
        snapshotCollector.reply(message, System.currentTimeMillis());
    }
}
