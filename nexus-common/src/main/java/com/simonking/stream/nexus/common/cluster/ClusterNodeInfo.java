package com.simonking.stream.nexus.common.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 节点注册信息
 *
 * <p>每个节点按心跳间隔把自己写进 {@code nexus:sse:nodes} Hash。
 * 其他节点/管理页读取时用 {@link #lastSeen} 判断是否在线：
 * 超过 {@code nodeTimeout} 未续期即视为下线——不需要额外的存活 Key，
 * 也不会因为进程被 kill 而留下「永远在线」的幽灵节点。
 *
 * @author simonking
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClusterNodeInfo {

    /**
     * 节点ID：集群内唯一，建议显式配置（node-1 / node-2）
     */
    private String nodeId;

    /**
     * 雪花ID 节点位：0~4095，集群内唯一
     *
     * <p>写进注册表是为了让「两个节点配了同一个 node-seq」在管理页一眼可见，
     * 也用于启动时的冲突检测。
     */
    private int nodeSeq;

    /**
     * 实例ID：每次进程启动重新生成
     *
     * <p>用来区分「同一 nodeId 重启」与「同一 nodeId 双开」：
     * 重启时旧记录的最后心跳早于本进程启动，可以直接接管；
     * 双开时旧实例在本次启动后仍在续期，必须拒绝。
     */
    private String instanceId;

    /**
     * 对外展示地址（host:port），管理页排障用
     */
    private String address;

    /**
     * 进程启动时刻（毫秒）
     */
    private long startTime;

    /**
     * 最后一次心跳时刻（毫秒），离线判定依据
     */
    private long lastSeen;

    /**
     * 当前本节点连接数
     */
    private int clientCount;

    /**
     * 本节点各业务模块连接数快照
     */
    private Map<String, Integer> modules;

    /**
     * 服务版本，便于滚动发布时区分节点
     */
    private String version;
}
