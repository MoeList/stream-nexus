package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.cluster.ClusterMessageType;
import com.simonking.stream.nexus.common.cluster.ClusterNodeInfo;
import com.simonking.stream.nexus.sse.admin.ConnectionViewMapper;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.connection.SseClient;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * 管理台账聚合：让管理页一次请求看到全集群的连接
 *
 * <p>为什么不用 owners Hash 直接算：它只有 {@code clientId -> nodeId}，没有 IP / 模块 / 心跳，
 * 管理页要展示的字段得再逐个节点去问，反而更绕。这里沿用「请求 - 回复」：
 * <ol>
 *     <li>管理节点生成 {@code requestId}，把 {@code replyKey} 一起广播到控制频道；</li>
 *     <li>各节点把<b>本节点一整批</b>连接视图 {@code RPUSH} 到 {@code replyKey}（一条消息一批，
 *         不是一个连接一条：节点多、连接多时能少一个数量级的往返）；</li>
 *     <li>管理节点轮询 {@code LLEN} 直到收齐在线节点数或超 {@code admin-timeout}，
 *         一次 {@code LRANGE} 取回、展平、删除 Key。</li>
 * </ol>
 *
 * <p><b>为什么向自己也算一次广播</b>：发起方在发布前先直接写了自己那份，且
 * {@link #reply} 会跳过「发起方就是本节点」的请求——否则本节点的连接会在表里出现两遍。
 * 这样做还有个好处：本节点数据不依赖 Pub/Sub 回环，订阅端抖动时至少能看到自己这台。
 *
 * <p><b>超时即部分</b>：一个卡死的节点不能拖垮整个管理页。等不到就返回已收到的部分，
 * 由调用方标记 {@code partial} 并把缺失节点回显出来——「少了几条但知道少了谁」
 * 比「转圈 30 秒然后 500」有用得多。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
public class ClusterSnapshotCollector {

    /**
     * 轮询间隔：{@code admin-timeout} 默认 500ms，20ms 一次既不空转也能压住延迟
     *
     * <p>用轮询而不是阻塞式 BLPOP：回复是「N 个节点各一条」，收齐条件依赖计数，
     * 阻塞读还得处理「读多了 / 读少了」，不如一个轻量计数循环清楚。
     */
    private static final long POLL_INTERVAL_MILLIS = 20L;

    private final StringRedisTemplate redis;

    private final ClusterProperties properties;

    private final ClusterNodeIdentity identity;

    private final ClusterNodeRegistry nodeRegistry;

    private final SseClientRegistry clients;

    private final ConnectionViewMapper viewMapper;

    private final ClusterBus bus;

    private final ObjectMapper objectMapper;

    /**
     * 收集全集群连接视图（管理页调用，单次最多阻塞 {@code admin-timeout}）
     *
     * <p>Redis 不可用时降级为「只有本节点」并标记 {@code partial}：
     * 管理页至少还能对本地连接做排查，而不是整页报错。
     */
    public Snapshot collect(long now) {
        String requestId = UUID.randomUUID().toString();
        String replyKey = ClusterConstants.keySnapshot(properties.getKeyPrefix(), requestId);

        List<String> expectedIds = nodeRegistry.nodes().stream()
                .map(ClusterNodeInfo::getNodeId)
                .sorted()
                .toList();
        // 注册表里没有自己（心跳还没写成功等极端情况）时也至少等自己一份，否则永远只能返回空
        int expected = Math.max(expectedIds.size(), 1);

        try {
            replyTo(replyKey, now);
            bus.publishControl(ClusterMessage.builder()
                    .type(ClusterMessageType.SNAPSHOT_REQUEST)
                    .originNodeId(identity.getNodeId())
                    .requestId(requestId)
                    .replyKey(replyKey)
                    .ts(now)
                    .build());
        } catch (Exception e) {
            log.warn("[sse] 快照请求发布失败，本次只返回本节点数据：{}", e.getMessage());
            return new Snapshot(localItems(now), expected, 0, expectedIds, true);
        }

        try {
            await(replyKey, expected);
            return aggregate(replyKey, expected, expectedIds);
        } catch (Exception e) {
            log.warn("[sse] 读取集群连接快照失败，本次只返回本节点数据：{}", e.getMessage());
            return new Snapshot(localItems(now), expected, 0, expectedIds, true);
        } finally {
            cleanup(replyKey);
        }
    }

    /**
     * 响应快照请求：把本节点连接写入 {@code replyKey}
     *
     * <p>异常直接抛给分发器：一条回复失败不该影响其他集群消息的处理，
     * 由上层统一记日志。反过来说，这里吞掉异常会让发起方一直等到超时。
     */
    public void reply(ClusterMessage message, long now) {
        String replyKey = message.getReplyKey();
        if (replyKey == null || replyKey.isBlank()) {
            log.warn("[sse] 快照请求缺少 replyKey，已忽略：requestId={}", message.getRequestId());
            return;
        }
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            // 发起方在 collect() 里已经直接写过自己那份，再写一次会让本节点连接出现两遍
            return;
        }
        replyTo(replyKey, now);
    }

    /**
     * 等收齐 {@code expected} 份回复，或到达 {@code admin-timeout}
     */
    private void await(String replyKey, int expected) {
        long deadline = System.currentTimeMillis() + Math.max(properties.getAdminTimeout().toMillis(), 0);
        while (true) {
            Long size = redis.opsForList().size(replyKey);
            if (size != null && size >= expected) {
                return;
            }
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) {
                return;
            }
            try {
                Thread.sleep(Math.min(POLL_INTERVAL_MILLIS, remain));
            } catch (InterruptedException e) {
                // 管理请求线程被中断：别把中断吞掉，尽快返回已收到的部分
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 读回并展平各节点快照
     *
     * <p>按 nodeId 去重：万一某个节点因为订阅重连收到两次广播，它的连接只会算一遍。
     */
    private Snapshot aggregate(String replyKey, int expected, List<String> expectedIds) {
        List<String> raw = redis.opsForList().range(replyKey, 0, -1);
        Map<String, NodeSnapshot> byNode = new TreeMap<>();
        int broken = 0;
        if (raw != null) {
            for (String body : raw) {
                try {
                    NodeSnapshot snapshot = objectMapper.readValue(body, NodeSnapshot.class);
                    if (snapshot == null || snapshot.nodeId() == null) {
                        broken++;
                        continue;
                    }
                    byNode.putIfAbsent(snapshot.nodeId(), snapshot);
                } catch (Exception e) {
                    // 单份损坏不该让整页失去数据：跳过并计数，下面的 missingNodes 仍能反映缺口
                    broken++;
                    log.warn("[sse] 节点连接快照解析失败，已跳过：{}", e.getMessage());
                }
            }
        }

        List<Map<String, Object>> items = new ArrayList<>();
        for (NodeSnapshot snapshot : byNode.values()) {
            if (snapshot.items() != null) {
                items.addAll(snapshot.items());
            }
        }
        List<String> missing = expectedIds.stream()
                .filter(id -> !byNode.containsKey(id))
                .toList();
        if (!missing.isEmpty() || broken > 0) {
            log.debug("[sse] 连接快照不完整：已收到 {}/{} 个节点，缺失 {}，损坏 {} 份",
                    byNode.size(), expected, missing, broken);
        }
        return new Snapshot(List.copyOf(items), expected, byNode.size(), missing, !missing.isEmpty());
    }

    private void replyTo(String replyKey, long now) {
        List<Map<String, Object>> items = localItems(now);
        redis.opsForList().rightPush(replyKey,
                objectMapper.writeValueAsString(new NodeSnapshot(identity.getNodeId(), now, items)));
        // 每次回复都续期：发起方途中崩溃也不会留下永久垃圾 Key
        redis.expire(replyKey, properties.getSnapshotTtl());
        log.debug("[sse] 已写入连接快照：{} 条，key={}", items.size(), replyKey);
    }

    private List<Map<String, Object>> localItems(long now) {
        List<Map<String, Object>> items = new ArrayList<>(clients.size());
        for (SseClient client : clients.all()) {
            items.add(viewMapper.toClusterView(client, now, identity.getNodeId()));
        }
        return items;
    }

    private void cleanup(String replyKey) {
        try {
            redis.delete(replyKey);
        } catch (Exception e) {
            // 删不掉不影响本次结果，TTL 会兜底；只在 debug 留痕，别让运维以为出了大事
            log.debug("[sse] 清理快照 List 失败（有 TTL 兜底）：{}", e.getMessage());
        }
    }

    /**
     * 聚合结果
     *
     * @param items         全集群连接视图（每条已带 {@code nodeId}）
     * @param expectedNodes 期望回复的节点数（不低于 1）
     * @param receivedNodes 实际回复的节点数
     * @param missingNodes  未回复的节点ID
     * @param partial       是否不完整：{@code missingNodes} 非空即为真
     */
    public record Snapshot(List<Map<String, Object>> items,
                           int expectedNodes,
                           int receivedNodes,
                           List<String> missingNodes,
                           boolean partial) {
    }

    /**
     * 单个节点的快照批次
     *
     * <p>用「一批」而不是「一个连接一条」：节点回复次数从 O(连接数) 降到 O(节点数)，
     * 空节点也能回一条空批次——发起方据此区分「这个节点没连接」和「这个节点没响应」。
     */
    public record NodeSnapshot(String nodeId, long ts, List<Map<String, Object>> items) {
    }
}
