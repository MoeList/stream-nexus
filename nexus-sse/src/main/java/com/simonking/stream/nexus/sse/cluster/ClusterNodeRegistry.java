package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import com.simonking.stream.nexus.common.cluster.ClusterNodeInfo;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 节点注册表：集群内的「谁在线」
 *
 * <p>每个节点把自己写进 {@code {ns}:nodes} Hash（field = nodeId），
 * 每 {@code node-heartbeat-interval} 续期一次；读取方按 {@code node-timeout}
 * 判断在线——没有额外的存活 Key，进程被 kill 后最多 {@code node-timeout}
 * 就会从台账里消失。
 *
 * <p><b>启动顺序</b>：先做唯一性校验再写自己的记录。
 * 校验不通过直接抛异常让进程退出（{@link ApplicationRunner} 的异常会终结启动），
 * 宁可这一个副本起不来，也不能让两个节点用同一个节点位生成消息ID——
 * 那种故障在运行期只表现为「客户端偶尔收到重复消息」，几乎无法定位。
 *
 * <p><b>重启 vs 双开</b>：两者在注册表里都表现为「同一 nodeId、不同 instanceId」。
 * 区别在于旧记录的最后心跳是否落在本次进程启动之后：
 * 落在之后说明旧实例此刻还在续期，是双开；落在之前说明它已经停了，只是记录还没过期。
 *
 * <p><b>时钟</b>：{@code lastSeen} 用本机时间写入、本机时间比较。
 * 节点间时钟偏差超过 {@code node-timeout} 时会误判，前提是各机器开着 NTP；
 * 不用 Redis TIME 兜底是为了让心跳只读一次 Hash，不额外增加往返。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "true")
public class ClusterNodeRegistry implements ApplicationRunner, DisposableBean {

    /**
     * 版本号：jar 运行时取 MANIFEST 里的 Implementation-Version，IDE 里为 null
     */
    private static final String VERSION = resolveVersion();

    private final StringRedisTemplate redis;

    private final ClusterProperties properties;

    private final ClusterNodeIdentity identity;

    private final SseClientRegistry clients;

    private final ObjectMapper objectMapper;

    /**
     * 在线节点快照：心跳时刷新，推送路径直接读它（{@code clusterNodes} 字段）
     */
    private volatile List<ClusterNodeInfo> snapshot = List.of();

    /**
     * 是否已完成「校验 + 首次注册」：心跳在注册完成前不写 Redis，
     * 避免调度器抢在唯一性校验之前把身份写进去
     */
    private volatile boolean registered = false;

    @Override
    public void run(ApplicationArguments args) {
        ClusterNodeInfo self = selfInfo();
        String conflict = detectConflict(self, readAll(), System.currentTimeMillis(),
                properties.getNodeTimeout().toMillis());
        if (conflict != null) {
            throw new IllegalStateException("[sse] 节点身份校验失败，拒绝启动：" + conflict);
        }

        publishSelf();
        registered = true;
        refreshSnapshot();
        log.info("[sse] 节点注册完成：nodeId={}, nodeSeq={}, 在线节点={}",
                identity.getNodeId(), identity.getNodeSeq(), snapshot.size());
    }

    /**
     * 周期心跳：续期自己的记录，并刷新在线快照
     *
     * <p>异常只 warn 不抛：Redis 抖动一次不该让调度器把这个任务当成失败终止，
     * 下个周期重试即可；连续失败最终会由 {@code node-timeout} 把本节点判为离线。
     */
    @Scheduled(fixedDelayString = "${nexus.sse.cluster.node-heartbeat-interval:5000}")
    public void heartbeat() {
        if (!registered) {
            return;
        }
        try {
            publishSelf();
            refreshSnapshot();
        } catch (Exception e) {
            log.warn("[sse] 节点心跳失败，下个周期重试：{}", e.getMessage());
        }
    }

    /**
     * 实时读取节点列表（管理页用）
     *
     * <p>顺便清理过期记录。Redis 不可用时降级为上一次快照，不让管理页整体报错。
     */
    public List<ClusterNodeInfo> nodes() {
        try {
            refreshSnapshot();
        } catch (Exception e) {
            log.warn("[sse] 读取节点列表失败，返回上次快照：{}", e.getMessage());
        }
        return snapshot;
    }

    /**
     * 在线节点数（含本节点）：供 {@code PushResult.clusterNodes} 使用
     */
    public int onlineCount() {
        return snapshot.size();
    }

    /**
     * 优雅下线：注销节点记录 + 清理本节点的连接归属
     *
     * <p>best-effort：进程被 kill -9 时不会走到这里，由 {@link #refreshSnapshot()}
     * 在发现本节点过期时连带清理（见 {@link #cleanOwnersOf}）。
     */
    @Override
    public void destroy() {
        if (!registered) {
            return;
        }
        try {
            redis.opsForHash().delete(key(), identity.getNodeId());
            int cleaned = cleanOwnersOf(List.of(identity.getNodeId()));
            log.info("[sse] 节点已注销：nodeId={}, 清理连接归属 {} 条", identity.getNodeId(), cleaned);
        } catch (Exception e) {
            log.warn("[sse] 节点注销失败（依赖 node-timeout 兜底）：{}", e.getMessage());
        }
    }

    /**
     * 唯一性校验（纯函数，便于单测）
     *
     * @return null 表示无冲突；否则返回可直接打印的原因
     */
    static String detectConflict(ClusterNodeInfo self, List<ClusterNodeInfo> others, long now, long timeout) {
        for (ClusterNodeInfo other : others) {
            if (other == null || other.getNodeId() == null) {
                continue;
            }
            // 超过 node-timeout 未续期：视为已下线，它的 nodeSeq 可以复用
            if (now - other.getLastSeen() > timeout) {
                continue;
            }
            if (other.getNodeId().equals(self.getNodeId())) {
                // 旧记录的最后心跳早于本进程启动 → 是重启，直接接管；
                // 晚于本进程启动 → 旧实例此刻仍在续期，是双开
                boolean overlap = other.getLastSeen() >= self.getStartTime();
                if (overlap && !Objects.equals(other.getInstanceId(), self.getInstanceId())) {
                    return "节点ID 双开：nodeId=" + self.getNodeId()
                            + " 已有实例在运行（instanceId=" + other.getInstanceId()
                            + ", lastSeen=" + other.getLastSeen() + "）";
                }
                continue;
            }
            if (other.getNodeSeq() == self.getNodeSeq()) {
                return "节点位冲突：nodeSeq=" + self.getNodeSeq()
                        + " 已被在线节点 " + other.getNodeId() + " 占用";
            }
        }
        return null;
    }

    private void publishSelf() {
        redis.opsForHash().put(key(), identity.getNodeId(),
                objectMapper.writeValueAsString(selfInfo()));
    }

    /**
     * 读取全部节点，剔除过期与损坏记录并回写删除
     */
    private void refreshSnapshot() {
        long now = System.currentTimeMillis();
        long timeout = properties.getNodeTimeout().toMillis();
        Map<Object, Object> raw = redis.opsForHash().entries(key());

        List<ClusterNodeInfo> alive = new ArrayList<>(raw.size());
        List<Object> expired = new ArrayList<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            ClusterNodeInfo info = parse(entry.getValue());
            // nodeId 缺失同样按损坏记录处理：下面按 nodeId 排序，null 会让整个心跳线程抛 NPE
            if (info == null || info.getNodeId() == null || now - info.getLastSeen() > timeout) {
                expired.add(entry.getKey());
            } else {
                alive.add(info);
            }
        }

        if (!expired.isEmpty()) {
            try {
                redis.opsForHash().delete(key(), expired.toArray());
                // 被强杀/崩溃的节点走不到 destroy，它的连接归属会永久留在 owners 里，
                // 让 PONG 一直转发到一个没人订阅的节点频道。这里趁发现它过期时一并清掉：
                // 代价是 O(连接数) 的一次反查，只在节点下线的那一个周期发生。
                List<String> dead = expired.stream().map(String::valueOf).toList();
                int cleaned = cleanOwnersOf(dead);
                log.info("[sse] 清理离线/损坏节点记录：{}，连带清理连接归属 {} 条", dead, cleaned);
            } catch (Exception e) {
                log.warn("[sse] 清理离线节点失败：{}", e.getMessage());
            }
        }

        alive.sort(Comparator.comparing(ClusterNodeInfo::getNodeId));
        snapshot = List.copyOf(alive);
    }

    private List<ClusterNodeInfo> readAll() {
        Map<Object, Object> raw = redis.opsForHash().entries(key());
        List<ClusterNodeInfo> result = new ArrayList<>(raw.size());
        for (Object value : raw.values()) {
            ClusterNodeInfo info = parse(value);
            if (info != null && info.getNodeId() != null) {
                result.add(info);
            }
        }
        return result;
    }

    /**
     * 删除 owners 里归属指定节点的字段
     *
     * <p>节点正常下线由 {@link #destroy()} 调用；异常退出则由 {@link #refreshSnapshot()}
     * 在发现该节点过期后调用——两种情况都必须清，否则残留字段会把 PONG 转发到
     * 一个已经不存在的节点。owner 存的是 nodeId，所以只能按 value 反查一次，
     * 代价 O(连接数)，不在热路径上。
     */
    private int cleanOwnersOf(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return 0;
        }
        Map<Object, Object> owners = redis.opsForHash().entries(ownersKey());
        List<Object> mine = new ArrayList<>();
        for (Map.Entry<Object, Object> entry : owners.entrySet()) {
            if (nodeIds.contains(String.valueOf(entry.getValue()))) {
                mine.add(entry.getKey());
            }
        }
        if (mine.isEmpty()) {
            return 0;
        }
        redis.opsForHash().delete(ownersKey(), mine.toArray());
        return mine.size();
    }

    private ClusterNodeInfo selfInfo() {
        return ClusterNodeInfo.builder()
                .nodeId(identity.getNodeId())
                .nodeSeq(identity.getNodeSeq())
                .instanceId(identity.getInstanceId())
                .address(identity.getAddress())
                .startTime(identity.getStartTime())
                .lastSeen(System.currentTimeMillis())
                .clientCount(clients.size())
                .modules(clients.moduleStats())
                .version(VERSION)
                .build();
    }

    private ClusterNodeInfo parse(Object raw) {
        try {
            return objectMapper.readValue(String.valueOf(raw), ClusterNodeInfo.class);
        } catch (Exception e) {
            log.warn("[sse] 节点注册信息解析失败，按损坏记录清理：{}", raw);
            return null;
        }
    }

    private String key() {
        return ClusterConstants.keyNodes(properties.getKeyPrefix());
    }

    private String ownersKey() {
        return ClusterConstants.keyOwners(properties.getKeyPrefix());
    }

    private static String resolveVersion() {
        String version = ClusterNodeRegistry.class.getPackage().getImplementationVersion();
        return version == null ? "dev" : version;
    }
}
