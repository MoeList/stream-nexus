package com.simonking.nexus.websocket.cluster;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.stream.nexus.common.util.IdGenerator;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.UUID;

/**
 * 节点身份
 *
 * <p>启动时确定三件事，之后只读：
 * <ul>
 *     <li>{@link #nodeId}：节点唯一标识，用于节点注册表、PUSH 回环跳过；</li>
 *     <li>{@link #nodeSeq}：雪花ID 的 12 位节点位，集群内唯一；</li>
 *     <li>{@link #address}：管理页展示用的 host:port。</li>
 * </ul>
 *
 * <p><b>端口口径</b>：nodeId 与 address 都取对外接入端口 {@code nexus.ws.ws-port}（默认 9090），
 * 而不是 HTTP 管理端口。理由与 SSE 取 {@code server.port} 一致——同一个 Redis 里排障时，
 * 看到 {@code host-9090} 就知道是「终端连的那个 WebSocket 节点」。
 *
 * <p>单机模式同样会初始化：nodeSeq 默认 0，nodeId 用 host-port。
 * 这样「单机 → 集群」只改配置，不换 ID 格式。
 *
 * @author simonking
 */
@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class WsClusterNodeIdentity {

    private static final int MAX_NODE_SEQ = (1 << 12) - 1;

    private final WsClusterProperties properties;

    @Value("${nexus.ws.ws-port:9090}")
    private int wsPort;

    private String nodeId;

    private int nodeSeq;

    private String address;

    /**
     * 本次进程的唯一标识：用于区分「重启」与「同 nodeId 双开」
     */
    private String instanceId;

    /**
     * JVM 启动时刻：判定双开时，旧记录的最后心跳是否落在本次启动之后
     */
    private long startTime;

    @PostConstruct
    public void init() {
        this.nodeId = sanitize(resolveNodeId());
        this.nodeSeq = resolveNodeSeq();
        this.address = resolveAddress();
        this.instanceId = UUID.randomUUID().toString();
        this.startTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        IdGenerator.configureNode(nodeSeq);
        log.info("[ws] 节点身份：nodeId={}, nodeSeq={}, address={}, instance={}, cluster={}",
                nodeId, nodeSeq, address, instanceId.substring(0, 8), properties.isEnabled());
    }

    private String resolveNodeId() {
        String configured = properties.getNodeId();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return host() + "-" + wsPort;
    }

    private int resolveNodeSeq() {
        Integer configured = properties.getNodeSeq();
        if (configured == null) {
            // 单机固定 0，保证「单机 → 集群」切换前 ID 的节点位始终可预测；
            // 集群才需要按 nodeId 推导一个临时值（生产仍应显式配置）
            return properties.isEnabled() ? Math.floorMod(nodeId.hashCode(), MAX_NODE_SEQ + 1) : 0;
        }
        if (configured < 0 || configured > MAX_NODE_SEQ) {
            throw new IllegalStateException(
                    "nexus.ws.cluster.node-seq 必须在 0~" + MAX_NODE_SEQ + " 之间：" + configured);
        }
        return configured;
    }

    private String resolveAddress() {
        String configured = properties.getAdvertiseAddress();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return host() + ":" + wsPort;
    }

    /**
     * 容器里优先用编排注入的 HOSTNAME；本机运行时回退到 InetAddress
     */
    private String host() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.warn("[ws] 获取主机名失败，回退 localhost：{}", e.getMessage());
            return "localhost";
        }
    }

    /**
     * 只保留字母、数字、点、下划线、短横线：
     * nodeId 会拼进 Redis 频道名，出现空格/冒号等字符会导致订阅与发布对不上
     */
    private String sanitize(String raw) {
        String cleaned = raw == null ? "" : raw.replaceAll("[^A-Za-z0-9._-]", "-");
        return cleaned.isBlank() ? "node" : cleaned;
    }
}
