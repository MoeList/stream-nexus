package com.simonking.nexus.websocket.cluster;

import com.simonking.nexus.websocket.auth.PushAppRegistry;
import com.simonking.nexus.websocket.core.WsBroadcaster;
import com.simonking.nexus.websocket.registry.WsClientRegistry;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 集群消息分发
 *
 * <p>PUSH 只在本节点扇出，且跳过生产该消息的入口节点（入口节点已经本地扇出过）；
 * KICK / KICK_ALL 走注册表的统一回收入口，这样模块索引与 channel 会一起清理。
 *
 * <p><b>与 SSE 的差异</b>
 * <ul>
 *     <li>没有 PONG 分支：WebSocket 的心跳应答走同一条 TCP 连接，必然回到持有连接的节点；</li>
 *     <li>KICK 用 {@link WsClientRegistry#kick} 而不是 {@code remove}：先给终端发一个
 *         {@code KICKED} 关闭帧，终端能区分「被管理员下线」和「网络断了」，
 *         前者不应立刻自动重连。</li>
 * </ul>
 *
 * <p>{@code APPS_CHANGED} 让其他节点立即重载推送应用台账（不等 {@code app-cache-ttl}）；
 * {@code SNAPSHOT_REQUEST} 交给 {@link WsClusterSnapshotCollector} 回写本节点连接快照。
 * 两者都不产生推送副作用，收到重复广播是幂等的。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
public class WsClusterDispatcher {

    private final WsBroadcaster broadcaster;

    private final WsClientRegistry registry;

    private final WsClusterNodeIdentity identity;

    private final PushAppRegistry appRegistry;

    private final WsClusterSnapshotCollector snapshotCollector;

    public void dispatch(ClusterMessage message) {
        switch (message.getType()) {
            case PUSH -> onPush(message);
            case KICK -> onKick(message);
            case KICK_ALL -> onKickAll(message);
            case APPS_CHANGED -> onAppsChanged(message);
            case SNAPSHOT_REQUEST -> onSnapshotRequest(message);
            case PONG -> log.debug("[ws] 收到 PONG 指令但 WebSocket 无连接归属索引，忽略");
            default -> log.warn("[ws] 未知集群消息类型：{}", message.getType());
        }
    }

    private void onPush(ClusterMessage message) {
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            // 本节点就是入口节点，发布前已经本地扇出过；再发一次就是重复推送
            return;
        }
        broadcaster.pushLocal(message);
    }

    private void onKick(ClusterMessage message) {
        String clientId = message.getClientId();
        if (!StringUtils.hasText(clientId)) {
            return;
        }
        // action 承载踢下线原因，便于终端在关闭帧里看到「谁踢的、为什么」
        String reason = StringUtils.hasText(message.getAction()) ? message.getAction() : "kicked by admin";
        log.debug("[ws] 收到集群踢下线指令，clientId={}, from={}", clientId, message.getOriginNodeId());
        registry.kick(clientId, reason);
    }

    private void onKickAll(ClusterMessage message) {
        int removed = registry.removeAll();
        if (removed > 0) {
            log.info("[ws] 集群指令强制下线本节点全部连接：{} 条", removed);
        }
    }

    /**
     * 台账变更：其他节点改完凭证后，本节点立即重载，不必等 {@code app-cache-ttl}
     */
    private void onAppsChanged(ClusterMessage message) {
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            // 写节点自己已经更新过内存，不用再读一遍整表
            return;
        }
        appRegistry.reload();
    }

    /**
     * 快照请求：把本节点连接写入 replyKey
     *
     * <p>发起方自己那份在 collect() 里已直接写过，{@code reply()} 会跳过回环。
     */
    private void onSnapshotRequest(ClusterMessage message) {
        snapshotCollector.reply(message, System.currentTimeMillis());
    }
}
