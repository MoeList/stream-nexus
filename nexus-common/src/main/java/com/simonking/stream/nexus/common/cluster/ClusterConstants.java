package com.simonking.stream.nexus.common.cluster;

/**
 * 集群总线协议常量（SSE / WebSocket 共用）
 *
 * <p>Redis 的 Key / Channel 采用「命名空间 + 固定后缀」：
 * {@code nexus:sse:bus:push}、{@code nexus:sse:nodes}、{@code nexus:sse:owners} 等。
 * 命名空间由各服务自己给（{@code nexus:sse} / {@code nexus:ws}），
 * 这样两个服务共用一个 Redis 时天然隔离，不会互相消费对方的广播。
 *
 * <p>本类只放<b>协议级字面量</b>：一旦改动，两个服务与所有节点必须同步发布，
 * 否则会出现「频道对不上、消息静默丢失」——编译器不会报错，故集中在此。
 *
 * @author simonking
 */
public final class ClusterConstants {

    private ClusterConstants() {
    }

    /**
     * 总线协议版本：集群消息信封里的 {@code v} 字段
     *
     * <p>升级不兼容的消息结构时递增。消费端发现版本不认识应记录告警并丢弃，
     * 避免把旧格式硬解成新对象、静默丢字段。
     */
    public static final int PROTOCOL_VERSION = 1;

    /**
     * 推送广播频道后缀：所有节点订阅，收到后各自在本节点扇出
     */
    public static final String CHANNEL_PUSH = ":bus:push";

    /**
     * 控制频道后缀：KICK / KICK_ALL / APPS_CHANGED / SNAPSHOT_REQUEST 等低频指令
     */
    public static final String CHANNEL_CONTROL = ":bus:control";

    /**
     * 节点点对点频道前缀：后接 nodeId，用于 PONG 定向转发等
     */
    public static final String CHANNEL_NODE_PREFIX = ":node:";

    /**
     * 节点注册表（Hash）：field = nodeId，value = {@code ClusterNodeInfo} JSON
     */
    public static final String KEY_NODES = ":nodes";

    /**
     * 连接归属索引（Hash）：field = clientId，value = nodeId，PONG 定向转发用
     */
    public static final String KEY_OWNERS = ":owners";

    /**
     * 推送应用台账（Hash）：field = appId，value = 应用 JSON
     */
    public static final String KEY_APPS = ":apps";

    /**
     * 推送应用台账版本号（String）：每次写台账 INCR，节点据此刷新本地缓存
     */
    public static final String KEY_APPS_VERSION = ":apps:version";

    /**
     * 管理台账快照响应（List）前缀：后接 requestId，由各节点 LPUSH，管理节点汇总后删除
     */
    public static final String KEY_SNAPSHOT_PREFIX = ":snapshot:";

    public static String channelPush(String namespace) {
        return namespace + CHANNEL_PUSH;
    }

    public static String channelControl(String namespace) {
        return namespace + CHANNEL_CONTROL;
    }

    public static String channelNode(String namespace, String nodeId) {
        return namespace + CHANNEL_NODE_PREFIX + nodeId;
    }

    public static String keyNodes(String namespace) {
        return namespace + KEY_NODES;
    }

    public static String keyOwners(String namespace) {
        return namespace + KEY_OWNERS;
    }

    public static String keyApps(String namespace) {
        return namespace + KEY_APPS;
    }

    public static String keyAppsVersion(String namespace) {
        return namespace + KEY_APPS_VERSION;
    }

    public static String keySnapshot(String namespace, String requestId) {
        return namespace + KEY_SNAPSHOT_PREFIX + requestId;
    }
}
