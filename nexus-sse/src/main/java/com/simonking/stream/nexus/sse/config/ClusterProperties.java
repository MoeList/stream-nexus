package com.simonking.stream.nexus.sse.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 集群模式配置
 *
 * <p>默认关闭：{@code nexus.sse.cluster.enabled=false} 时不会创建任何 Redis
 * 连接、不订阅任何频道，进程行为与改造前的单机版本完全一致。
 *
 * <p>集群模式的硬约束：
 * <ul>
 *     <li>{@code node-id} 必须集群内唯一，建议显式配置（node-1 / node-2）；</li>
 *     <li>{@code node-seq} 必须集群内唯一（0~4095），留空时按 nodeId 哈希推导——
 *         哈希有碰撞概率，生产环境务必显式配置；</li>
 *     <li>所有节点连接同一个 Redis，且 {@code key-prefix} 一致。</li>
 * </ul>
 *
 * @author simonking
 */
@Data
@ConfigurationProperties(prefix = "nexus.sse.cluster")
public class ClusterProperties {

    /**
     * 是否启用集群模式
     *
     * <p>false：纯单机，不加载 Redis 相关 Bean，不建立 Redis 连接
     */
    private boolean enabled = false;

    /**
     * 节点ID：集群内唯一
     *
     * <p>留空时用 {@code host-port} 兜底。容器部署请显式配置，
     * 否则多个副本的 hostname 可能相同（或都是 localhost）导致节点互相覆盖。
     */
    private String nodeId = "";

    /**
     * 节点位（雪花ID 的 12 位）：0 ~ 4095，集群内唯一
     *
     * <p>留空时按 {@code Math.floorMod(nodeId.hashCode(), 4096)} 推导。
     * 哈希只适合「节点数很少、且能接受碰撞排查」的场景，生产环境请显式配置。
     */
    private Integer nodeSeq;

    /**
     * Redis Key / Channel 命名空间
     *
     * <p>SSE 与 WebSocket 共用一个 Redis 时必须不同（{@code nexus:sse} / {@code nexus:ws}），
     * 否则两个服务会互相消费对方的广播。
     */
    private String keyPrefix = "nexus:sse";

    /**
     * 对外展示地址（host:port），仅用于管理页排障；留空时用 host-port
     */
    private String advertiseAddress = "";

    /**
     * 节点心跳间隔：把自己写进 {@code nodes} Hash 的频率
     */
    private Duration nodeHeartbeatInterval = Duration.ofSeconds(5);

    /**
     * 节点离线判定：超过该时间未续期即视为下线
     */
    private Duration nodeTimeout = Duration.ofSeconds(30);

    /**
     * 管理台账快照的等待上限
     *
     * <p>管理页请求连接列表时向各节点要快照，最多等这么久；
     * 超时返回已收到的部分，并在响应里标记 {@code partial=true} + 缺失节点，
     * 不让一个卡死的节点拖垮整个管理页。
     */
    private Duration adminTimeout = Duration.ofMillis(500);

    /**
     * 快照响应 List 的 TTL：防止管理节点在收集途中崩溃后留下垃圾 Key
     */
    private Duration snapshotTtl = Duration.ofSeconds(60);

    /**
     * 各节点本地推送应用台账缓存的刷新间隔
     */
    private Duration appCacheTtl = Duration.ofSeconds(30);

    /**
     * 启动时是否把本地 {@code push-apps.json} 导入 Redis 台账
     *
     * <p>只在「首次从单机迁移到集群」时打开一次：多节点同时打开会互相覆盖。
     */
    private boolean appsImportOnStartup = false;

    /**
     * 单条 Redis 消息的最大字节数
     *
     * <p>防止一次超大 payload 打爆订阅端内存。超过时入口节点直接拒绝推送，
     * 让调用方收到明确错误，而不是让消息在某个节点静默失败。
     */
    private int maxMessageBytes = 1024 * 1024;
}
